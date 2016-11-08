package rdfcube;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.MultiMap;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import rdfcube.data.InMemoryRDFCubeDataSource;
import rdfcube.data.RDFCubeDataSource;
import rdfcube.types.Quadruple;

public class PartitionLattice implements Iterable<RDFCubeFragment>{
	
	/**
	 * Root fragment, i.e., ancestor of all fragments in the lattice.
	 * It represents the entire cube
	 */
	private RDFCubeFragment root;
	
	/**
	 * Object containing the cube structure definition
	 */
	private RDFCubeStructure structure;
	
	/**
	 * Proxy to the actual cube data
	 */
	private RDFCubeDataSource data;
	
	/**
	 * Map from children to parents
	 */
	private MultiValuedMap<RDFCubeFragment, RDFCubeFragment> parentsGraph; 
	
	/**
	 * Map from parent fragments to children
	 */
	private MultiValuedMap<RDFCubeFragment, RDFCubeFragment> childrenGraph;
	
	/**
	 * Map from data fragments to metadata fragments
	 */
	private MultiValuedMap<RDFCubeFragment, RDFCubeFragment> metadataMap;
	
	/**
	 * Map from signature hash codes to partitions.
	 */
	private Map<Quadruple<String, String, String, String>, RDFCubeFragment> partitionsFullSignatureMap;
	
	/**
	 * Map where the keys are data types and the values are all the fragments
	 * whose signature relation has that type as domain.
	 */
	private MultiValuedMap<String, RDFCubeFragment> partitionsDomainOfSignatureMap;

	/**
	 * Map where the keys are data types and the values are all the fragments
	 * whose signature relation has that type as range.
	 */	
	private MultiValuedMap<String, RDFCubeFragment> partitionsRangeOfSignatureMap;
		
	
	private PartitionLattice(RDFCubeFragment root, RDFCubeStructure schema, RDFCubeDataSource data) {
		this.root = root;
		this.structure = schema;
		this.data = data;
		parentsGraph = new HashSetValuedHashMap<>();
		childrenGraph = new HashSetValuedHashMap<>();
		metadataMap = new HashSetValuedHashMap<>();
		partitionsFullSignatureMap = new LinkedHashMap<>();
		partitionsDomainOfSignatureMap = new HashSetValuedHashMap<>();
		partitionsRangeOfSignatureMap = new HashSetValuedHashMap<>();
	}
	
	/**
	 * Builds a partition lattice from the schema and the triples of an RDF data cube
	 * @param schemaPath
	 * @param dataPath
	 * @return
	 */
	public static PartitionLattice build(RDFCubeStructure schema, RDFCubeDataSource data) {
		RDFCubeFragment root = createFragment(); 
		PartitionLattice lattice = new PartitionLattice(root, schema, data);
		
		Iterator<Quadruple<String, String, String, String>> iterator = data.iterator();
		// Register all the triples in the fragments
		while (iterator.hasNext()) {
			lattice.registerTuple(iterator.next());
		}
		
		// Create the metadata relations between the fragments
		lattice.linkData2MetadataFragments();
			
		return lattice;
	}
	
	private void linkData2MetadataFragments() {
		for (RDFCubeFragment fragment : parentsGraph.keySet()) {
			Set<RDFCubeFragment> ancestors = getAncestors(fragment);
			if (!fragment.isMetadata()) {
				// Get all the fragments joining on the object
				Quadruple<String, String, String, String> signature = fragment.getFirstSignature();
				String domain = signature.getFirst();
				Set<RDFCubeFragment> candidateMetadataFragments = (Set<RDFCubeFragment>) partitionsRangeOfSignatureMap.get(domain);
				if (candidateMetadataFragments != null) {
					for (RDFCubeFragment candidateFragment : candidateMetadataFragments) {
						if (candidateFragment.isMetadata()) {
							metadataMap.put(fragment, candidateFragment);
							for (RDFCubeFragment ancestor : ancestors) {
								metadataMap.put(ancestor, candidateFragment);
							}
						}
					}
				}
			}
		}
		
	}

	private Set<RDFCubeFragment> getAncestors(RDFCubeFragment fragment) {
		Set<RDFCubeFragment> parents = (Set<RDFCubeFragment>) parentsGraph.get(fragment);
		Set<RDFCubeFragment> result = new LinkedHashSet<>();
		if (parents != null) {
			for (RDFCubeFragment parent : parents) {
				result.add(parent);
				result.addAll(getAncestors(parent));
			}
		}
		
		return result;
	}

	/**
	 * Returns an RDF Data fragment which can be used as the root for a fragment lattice.
	 * @return
	 */
	private static RDFCubeFragment createFragment() {
		return new RDFCubeDataFragment();
	}
	
	private static RDFCubeFragment createFragment(String provenanceIdentifier) {
		return new RDFCubeDataFragment(provenanceIdentifier);
	}
	

	private RDFCubeFragment createFragment(Quadruple<String, String, String, String> relationSignature) {
		String relation = relationSignature.getSecond();
		if (structure.isMetadataRelation(relation)) {
			return new RDFCubeMetadataFragment(relationSignature);
		} else {
			return new RDFCubeDataFragment(relationSignature);
		}
	}
	

	
	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(root);
		strBuilder.append("\n");
		for (RDFCubeFragment partition : (Set<RDFCubeFragment>)parentsGraph.keySet()) {
			strBuilder.append(partition + "---->" + parentsGraph.get(partition) + "\n");	
		}
		strBuilder.append(metadataMap + "\n");
		return strBuilder.toString();
		
	}
	
	private void registerTuple(Quadruple<String, String, String, String> quad) {
		root.increaseSize();
		String provenanceIdentifier = quad.getFourth();
		
		// Register the triple in the fragment corresponding to the provenance identifier
		Quadruple<String, String, String, String> provSignature = new Quadruple<>(null, null, null, provenanceIdentifier);
		RDFCubeFragment provPartition = partitionsFullSignatureMap.get(provSignature);
		if (provPartition == null) {
			provPartition = createFragment(provenanceIdentifier);
			partitionsFullSignatureMap.put(provSignature, provPartition);
			addEdge(provPartition);
		}
		provPartition.increaseSize();
		
		// Register the triple in the fragment corresponding to the provenance identifier
		String relation = quad.getSecond();
		Pair<String, String> relationDomainAndRange = structure.getSignature(relation);		
		Quadruple<String, String, String, String> relationSignature = new Quadruple<>(relationDomainAndRange.getLeft(), 
				relation, relationDomainAndRange.getRight(), provenanceIdentifier); 
		RDFCubeFragment relationPlusProvPartition = partitionsFullSignatureMap.get(relationSignature);
		if (relationPlusProvPartition == null) {
			relationPlusProvPartition = createFragment(relationSignature);
			partitionsFullSignatureMap.put(relationSignature, provPartition);
			addEdge(relationPlusProvPartition, provPartition);
			if (relationSignature.getFirst() != null) {
				partitionsDomainOfSignatureMap.put(relationSignature.getFirst(), relationPlusProvPartition);
			}
			if (relationSignature.getThird() != null) {
				partitionsRangeOfSignatureMap.put(relationSignature.getThird(), relationPlusProvPartition);
			}
			
		}
		relationPlusProvPartition.increaseSize();
	}

	private boolean addEdge(RDFCubeFragment child, RDFCubeFragment parent) {			
		boolean result = parentsGraph.put(child, parent);
		childrenGraph.put(parent, child);
		return result;
	}
	
	private boolean addEdge(RDFCubeFragment child) {
		return addEdge(child, root);
	}
	

	public static void main(String[] args) throws IOException {
		RDFCubeDataSource source = 
				InMemoryRDFCubeDataSource.build("/home/galarraga/workspace/CubeProvenance/input/wikipedia.cube.tsv");
		RDFCubeStructure schema = 
				RDFCubeStructure.build("/home/galarraga/workspace/CubeProvenance/input/wikipedia.schema.tsv");
		PartitionLattice lattice = PartitionLattice.build(schema, source);
		System.out.println(lattice);

	}

	@Override
	public Iterator<RDFCubeFragment> iterator() {
		// TODO Auto-generated method stub
		return new Iterator<RDFCubeFragment>() {
			@SuppressWarnings("unchecked")
			Iterator<RDFCubeFragment> it = parentsGraph.keySet().iterator();
			
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
