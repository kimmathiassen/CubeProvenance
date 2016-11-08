package rdfcube;

import java.util.Iterator;

import rdfcube.data.RDFCubeDataSource;
import rdfcube.data.RDFCubeStructure;
import rdfcube.types.Quadruple;

public class ExampleFragmentLatticeBuilder implements FragmentLatticeBuilder {

	@Override
	public FragmentLattice build(RDFCubeDataSource data, RDFCubeStructure schema) {
		RDFCubeFragment root = FragmentLattice.createFragment(); 
		FragmentLattice lattice = new FragmentLattice(root, schema, data);
		
		Iterator<Quadruple<String, String, String, String>> iterator = data.iterator();
		// Register all the triples in the fragments
		while (iterator.hasNext()) {
			lattice.registerTuple(iterator.next());
		}
		
		// Create the metadata relations between the fragments
		lattice.linkData2MetadataFragments();
			
		return lattice;
	}

}
