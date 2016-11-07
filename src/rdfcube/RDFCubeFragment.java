package rdfcube;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.lang3.tuple.Triple;

public class RDFCubeFragment {

	// Fragment definition
	private Set<Triple<String, String, String>> relationSignatures;
	
	private Set<String> provenanceIds;
	
	private boolean cubePartition;
	
	private long size;
	
	private boolean root;
	
	public RDFCubeFragment() {
		relationSignatures = new LinkedHashSet<>();
		provenanceIds = new LinkedHashSet<>();
		root = true;
		size = 0;
	}
	
	public RDFCubeFragment(Triple<String, String, String> relationSignature, String provenanceId, boolean isCubePartition) {
		relationSignatures = new LinkedHashSet<>();
		provenanceIds = new LinkedHashSet<>();
		relationSignatures.add(relationSignature);
		provenanceIds.add(provenanceId);
		setCubePartition(isCubePartition);
		root = false;
		size = 0;
	}
	
	public RDFCubeFragment(String provenanceId) {
		relationSignatures = new LinkedHashSet<>();
		provenanceIds = new LinkedHashSet<>();
		provenanceIds.add(provenanceId);
		root = false;
		size = 0;		
	}

	public boolean isCubePartition() {
		return cubePartition;
	}

	public void setCubePartition(boolean cubePartition) {
		this.cubePartition = cubePartition;
	}
	
	public boolean isRoot() {
		return root;
	}
	
	public boolean hasSignature(Triple<String, String, String> relation, String provenanceId) {
		if (relation == null) {
			return relationSignatures.isEmpty() && provenanceIds.contains(provenanceId);
		} else {
			return relationSignatures.contains(relation) && provenanceIds.contains(provenanceId);
		}
	}
	
	public long size() {
		return size;
	}
	
	public void increaseSize() {
		++size;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((provenanceIds == null) ? 0 : provenanceIds.hashCode());
		result = prime * result + ((relationSignatures == null) ? 0 : relationSignatures.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RDFCubeFragment other = (RDFCubeFragment) obj;
		if (provenanceIds == null) {
			if (other.provenanceIds != null)
				return false;
		} else if (!provenanceIds.equals(other.provenanceIds))
			return false;
		if (relationSignatures == null) {
			if (other.relationSignatures != null)
				return false;
		} else if (!relationSignatures.equals(other.relationSignatures))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		if (root)
			return "[All, " +  size + " triples]";
		else
			return "[" + relationSignatures +  " " + provenanceIds + "  " + size + " triples]"; 
	}

}
