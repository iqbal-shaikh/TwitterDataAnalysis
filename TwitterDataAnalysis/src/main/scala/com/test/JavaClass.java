package com.test;

import java.io.*;
import java.util.*;


public class JavaClass {
	public static void main(String[] args) throws Exception {
	Properties prop = new Properties();
	prop.load(new FileInputStream("resources/file.properties"));
	String user = prop.getProperty("username");
	//String pass = prop.getProperty("password");
	
	System.out.println(user);
	}	
}
