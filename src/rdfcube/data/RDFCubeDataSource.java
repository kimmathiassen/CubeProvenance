package rdfcube.data;

import rdfcube.types.Quadruple;

public interface RDFCubeDataSource extends Iterable<Quadruple<String, String, String, String>> {

	public Iterable<Quadruple<String, String, String, String>> getQuadsPerSubject(String subject);
	
	public Iterable<Quadruple<String, String, String, String>> getQuadsPerObject(String object);
}
