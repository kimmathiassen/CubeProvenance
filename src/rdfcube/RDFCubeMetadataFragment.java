package rdfcube;

import rdfcube.types.Quadruple;

public class RDFCubeMetadataFragment extends RDFCubeFragment {

	public RDFCubeMetadataFragment(Quadruple<String, String, String, String> relationSignature) {
		super(relationSignature);
	}

	@Override
	public boolean isMetadata() {
		return true;
	}

}
