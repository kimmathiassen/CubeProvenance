package rdfcube;

import java.util.Set;

/**
 * Interface defines a family of classes that implement a selection strategy 
 * for the cube fragments defined in a cube lattice.
 * @author galarraga
 *
 */
public interface FragmentsSelector {
	
	public Set<RDFCubeFragment> select(FragmentLattice lattice);

}
