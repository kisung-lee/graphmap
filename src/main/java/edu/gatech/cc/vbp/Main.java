package edu.gatech.cc.vbp;

import java.lang.reflect.Method;

public class Main {
	
	private static void usage() {
		System.err.println("Usage: Main <full class name> <class-specific parameters>");
	}

	private static String[] scrubArgs(String[] args) {
		String[] toReturn = new String[args.length-1];
		for (int i=1; i<args.length; i++)
		{
			toReturn[i-1] = args[i];
		}
		return toReturn;
	}

	@SuppressWarnings("rawtypes")
	private static Method findMain(Class clazz) throws Exception {
		Method[] methods = clazz.getMethods();
		for (int i=0; i<methods.length; i++)
		{
			if (methods[i].getName().equals("main"))
				return methods[i];
		}
		return null;
	}

	/**
	 * @param args
	 */
	@SuppressWarnings("rawtypes")
	public static void main(String[] args) {
		if (args.length < 1) {
			usage();
			return;
		}
		
		String className = args[0];
		
		try {
			Class stemClass = Class.forName(className);
			String[] newArgs = scrubArgs(args);
			Method mainMethod = findMain(stemClass);
			if(mainMethod == null) {
				usage();
				return;
			}
			mainMethod.invoke(null, new Object[] { newArgs });
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
