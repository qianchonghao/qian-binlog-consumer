package com.example.qianbinlogconsumerbak2.mysql.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ClassInfo {
    public static String ARRAY = "ARRAY";
    public static String OBJECT = "OBJECT";
    String className();
    String type() default OBJECT;
}
