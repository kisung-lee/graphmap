package edu.gatech.cc.vbp.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class TextFileManager {
	public static List<String> readTextFile(String slavesFilename) {
		List<String> slaves = new ArrayList<String>();
		try {
			FileReader fileReader = new FileReader(slavesFilename);
			BufferedReader reader = new BufferedReader(fileReader);
			String line = null;
			while ((line = reader.readLine()) != null) {
				slaves.add(line.trim());
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return slaves;
	}

	public static BufferedReader getBufferedReader(String filename) {
		try {
			FileReader fileReader = new FileReader(filename);
			return new BufferedReader(fileReader);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static BufferedWriter getTextFileWriter(String filename)
			throws UnsupportedEncodingException, FileNotFoundException {
		return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
				filename)));
	}

	public static BufferedWriter getTextFileWriter(String filename,
			boolean append) throws UnsupportedEncodingException,
			FileNotFoundException {
		return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
				filename, append)));
	}
}
