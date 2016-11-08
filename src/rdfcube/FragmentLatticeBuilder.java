package rdfcube;

import rdfcube.data.RDFCubeDataSource;
import rdfcube.data.RDFCubeStructure;

public interface FragmentLatticeBuilder {

	/**
	 * Builds a fragments lattice from a given cube and its corresponding structure.
	 * @param source
	 * @param structure
	 * @return
	 */
	public FragmentLattice build(RDFCubeDataSource source, RDFCubeStructure structure);
}
