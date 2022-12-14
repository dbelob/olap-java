package javax.jmi.model;

import javax.jmi.reflect.RefClass;

public interface ParameterClass extends RefClass {
    public Parameter createParameter();
    public Parameter createParameter(String name, String annotation, DirectionKind direction, MultiplicityType multiplicity);
}
