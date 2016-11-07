package rdfcube;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import rdfcube.data.InMemoryRDFCubeDataSource;
import rdfcube.data.RDFCubeDataSource;
import rdfcube.types.Quadruple;

public class PartitionLattice implements Iterable<RDFCubeFragment>{
		
	private RDFCubeFragment root;
	
	private RDFCubeStructure schema;
	
	private RDFCubeDataSource data;
	
	private MultiMap parenthoodGraph; 
	
	/**
	 * Map from signature hash codes to partitions.
	 */
	private MultiMap partitionsFullSignatureMap;
	
	private MultiMap partitionsDomainOfSignatureMap;
	
	private MultiMap partitionsRangeOfSignatureMap;
		
	
	private PartitionLattice(RDFCubeFragment root, RDFCubeStructure schema, RDFCubeDataSource data) {
		this.root = root;
		this.schema = schema;
		this.data = data;
		parenthoodGraph = new MultiValueMap();
		partitionsFullSignatureMap = new MultiValueMap();
		partitionsDomainOfSignatureMap = new MultiValueMap();
		partitionsRangeOfSignatureMap = new MultiValueMap();
	}
	
	/**
	 * Builds a partition lattice from the schema and the triples of an RDF data cube
	 * @param schemaPath
	 * @param dataPath
	 * @return
	 */
	public static PartitionLattice build(RDFCubeStructure schema, RDFCubeDataSource data) {
		RDFCubeFragment root = new RDFCubeFragment();
		PartitionLattice lattice = new PartitionLattice(root, schema, data);
		
		Iterator<Quadruple<String, String, String, String>> iterator = data.iterator();
		// Register all the triples in the fragments
		while (iterator.hasNext()) {
			lattice.registerTuple(iterator.next());
		}
			
		return lattice;
	}
	
	private int signature2HashCode(Triple<String, String, String> relation, String provenanceId) {
		int prime = 31;
		return prime * prime + prime * (relation != null ? relation.hashCode() : 0) + provenanceId.hashCode();
	}
	
	private RDFCubeFragment findPartitionBySignature(Triple<String, String, String> relation, 
			String provenanceId) {
		int hashCode = signature2HashCode(relation, provenanceId);
		Collection multiValues = (Collection) partitionsFullSignatureMap.get(hashCode);
		RDFCubeFragment searchedPartition = null;
		if (multiValues != null) {
			for (Object value : multiValues) {
				RDFCubeFragment partition = (RDFCubeFragment)value;
				if (partition.hasSignature(relation, provenanceId)) {
					searchedPartition = partition;
					break;
				}
			}
		}
		
		return searchedPartition;
	}
	
	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(root);
		strBuilder.append("\n");
		for (RDFCubeFragment partition : (Set<RDFCubeFragment>)parenthoodGraph.keySet()) {
			strBuilder.append(partition + "---->" + parenthoodGraph.get(partition) + "\n");	
		}
		return strBuilder.toString();
		
	}
	
	private void registerTuple(Quadruple<String, String, String, String> quad) {
		root.increaseSize();
		String provenanceIdentifier = quad.getFourth();
		
		// Register the triple in the fragment corresponding to the provenance identifier
		RDFCubeFragment provPartition = findPartitionBySignature(null, provenanceIdentifier);
		if (provPartition == null) {
			provPartition = new RDFCubeFragment(provenanceIdentifier);
			partitionsFullSignatureMap.put(signature2HashCode(null, provenanceIdentifier), provPartition);
			addEdge(provPartition);
		}
		provPartition.increaseSize();
		
		// Register the triple in the fragment corresponding to the provenance identifier
		String relation = quad.getSecond();
		Pair<String, String> relationDomainAndRange = schema.getSignature(relation);
		Triple<String, String, String> relationSignature = new MutableTriple<>(relationDomainAndRange.getLeft(), 
				relation, relationDomainAndRange.getRight()); 
		RDFCubeFragment relationPlusProvPartition = findPartitionBySignature(relationSignature, provenanceIdentifier);
		if (relationPlusProvPartition == null) {
			relationPlusProvPartition = new RDFCubeFragment(relationSignature, 
					provenanceIdentifier, schema.isCubeRelation(relation));
			partitionsFullSignatureMap.put(signature2HashCode(relationSignature, provenanceIdentifier), provPartition);
			addEdge(relationPlusProvPartition, provPartition);
		}
		relationPlusProvPartition.increaseSize();
	}

	private Set<RDFCubeFragment> findJoinPartitions(String subject, String provenanceId) {
		Set<RDFCubeFragment> result = new LinkedHashSet<>();
		// Get all other triples for this subject
		Iterable<Quadruple<String, String, String, String>> subjectJoins = data.getQuadsPerSubject(subject);
		if (subjectJoins == null)
			return result;
		
		// Then get all relations to construct the signatures
		for (Quadruple<String, String, String, String> joinQuads : subjectJoins) {
			String relation = joinQuads.getSecond();
			Pair<String, String> domainAndRange = schema.getSignature(relation);
			Triple<String, String, String> signature = new MutableTriple<>(domainAndRange.getLeft(), relation, domainAndRange.getRight());
			RDFCubeFragment p1 = findPartitionBySignature(signature, provenanceId);
			if (p1 != null) result.add(p1);			
			RDFCubeFragment p2 = findPartitionBySignature(null, provenanceId);			
			if (p2 != null) result.add(p2);
		}
		
		return result;
	}

	private boolean addEdge(RDFCubeFragment child, RDFCubeFragment parent) {
		Collection<RDFCubeFragment> elements = (Collection<RDFCubeFragment>) parenthoodGraph.get(child);
		if (elements == null || !elements.contains(parent)) {
			parenthoodGraph.put(child, parent);
			return true;
		}
		
		return false;	
	}
	
	private boolean addEdge(RDFCubeFragment child) {
		return addEdge(child, root);
	}
	

	public static void main(String[] args) throws IOException {
		RDFCubeDataSource source = 
				InMemoryRDFCubeDataSource.build("/home/galarraga/workspace/CubeFragmentation/input/wikipedia.cube.tsv");
		RDFCubeStructure schema = 
				RDFCubeStructure.build("/home/galarraga/workspace/CubeFragmentation/input/wikipedia.schema.tsv");
		PartitionLattice lattice = PartitionLattice.build(schema, source);
		System.out.println(lattice);

	}

	@Override
	public Iterator<RDFCubeFragment> iterator() {
		// TODO Auto-generated method stub
		return new Iterator<RDFCubeFragment>() {
			@SuppressWarnings("unchecked")
			Iterator<RDFCubeFragment> it = parenthoodGraph.keySet().iterator();
			
			boolean rootVisited = false;
			
			@Override
			public boolean hasNext() {
				if (rootVisited) {
					return it.hasNext();
				} else {
					return true;
				}
			}

			@Override
			public RDFCubeFragment next() {
				if (rootVisited) {
					return it.next();
				} else {
					rootVisited = true;
					return root;
				}
			}
			
		};
	}
		
}
