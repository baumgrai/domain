package com.icx.common.base;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * File and path helpers
 * 
 * @author baumgrai
 */
public abstract class CFile {

	// -------------------------------------------------------------------------
	// File path helpers
	// -------------------------------------------------------------------------

	/**
	 * Get list of elements of file path
	 * 
	 * @param file
	 *            file
	 * 
	 * @return list of path elements
	 */
	public static List<String> getPathElements(File file) {
		return Common.stringToList(file.getPath().replace("\\", "/"), Common.StringSep.SLASH);
	}

	/**
	 * Get file as path relative to specified directory
	 * <p>
	 * If file is not under directory absolute file path is returned
	 * 
	 * @param file
	 *            file
	 * @param dir
	 *            directory
	 * 
	 * @return file object with relative file path
	 */
	public static File getRelativeFilePath(File file, File dir) {

		String filePath = file.getAbsolutePath();
		String dirPath = dir.getAbsolutePath();

		if (filePath.startsWith(dirPath)) {
			return new File(filePath.substring(dirPath.length() + 1));
		}
		else {
			return file;
		}
	}

	/**
	 * Get current directory
	 * 
	 * @return current directory
	 */
	public static String getCurrentDir() {
		return new File(".").getAbsolutePath();
	}

	// -------------------------------------------------------------------------
	// Create file
	// -------------------------------------------------------------------------

	/**
	 * Create file including directory tree if it does not exist already
	 * 
	 * @param file
	 *            file to check/create
	 * 
	 * @return file object
	 * 
	 * @throws IOException
	 */
	public static File checkOrCreateFile(File file) throws IOException {

		if (!file.getParentFile().exists()) {
			file.getParentFile().mkdirs();
		}

		if (!file.exists() && !file.createNewFile()) {
			throw new IOException("Non-existing file " + file.getCanonicalPath() + " could not be created!");
		}

		if (!file.canWrite()) {
			throw new IOException("File " + file.getCanonicalPath() + " cannot be written!");
		}

		return file;
	}

	// -------------------------------------------------------------------------
	// Read and write files
	// -------------------------------------------------------------------------

	/**
	 * Read binary file
	 * 
	 * @param file
	 *            file object
	 * 
	 * @return byte contents
	 * 
	 * @throws IOException
	 *             if file not found or on IO error
	 */
	public static byte[] readBinary(File file) throws IOException {

		long size = file.length();
		InputStream is = new FileInputStream(file);
		byte[] bytes = new byte[(int) size];

		try {
			is.read(bytes, 0, (int) size);
		}
		finally {
			is.close();
		}

		return bytes;
	}

	/**
	 * Write binary file
	 * 
	 * @param file
	 *            file object
	 * @param bytes
	 *            bytes to write
	 * 
	 * @throws IOException
	 *             if file not found or on IO error
	 */
	public static void writeBinary(File file, byte[] bytes) throws IOException {

		if (bytes == null)
			return;

		OutputStream os = new FileOutputStream(file);
		try {
			os.write(bytes);
		}
		finally {
			os.close();
		}
	}

	public static final String UTF8_BOM = "\uFEFF";

	/**
	 * Read text file
	 * 
	 * @param file
	 *            file object
	 * @param encoding
	 *            encoding string (e.g. "UTF-8")
	 * 
	 * @return file contents
	 * 
	 * @throws IOException
	 *             if file not found or on IO error
	 */
	public static String readText(File file, String encoding) throws IOException {

		InputStreamReader isr = new InputStreamReader(new FileInputStream(file), encoding);
		BufferedReader br = new BufferedReader(isr);
		StringBuilder contents = new StringBuilder();

		try {
			boolean isFirstLine = true;
			String line = null;
			while ((line = br.readLine()) != null) {

				if (isFirstLine) {
					if (line.startsWith(UTF8_BOM)) { // Remove BOM if exists
						line = line.substring(1);
					}
					isFirstLine = false;
				}

				contents.append(line);
				contents.append(System.getProperty("line.separator"));
			}
		}
		finally {
			br.close();
			isr.close();
		}

		return contents.toString();
	}

	/**
	 * Write text file
	 * 
	 * @param file
	 *            file object
	 * @param text
	 *            text to write
	 * @param append
	 *            if true append text at file end, if false overwrite contents
	 * @param encoding
	 *            encoding string (e.g. "UTF-8")
	 * 
	 * @throws IOException
	 *             if file not found or on IO error
	 */
	public static void writeText(File file, String text, boolean append, String encoding) throws IOException {

		if (Common.isEmpty(text)) {
			return;
		}

		try (OutputStream fout = new FileOutputStream(file, append); OutputStream bout = new BufferedOutputStream(fout); OutputStreamWriter out = new OutputStreamWriter(bout, encoding);) {
			out.write(text);
		}
	}

	// -------------------------------------------------------------------------
	// Find files
	// -------------------------------------------------------------------------

	/**
	 * File search filter for subdirectories
	 */
	private static class SubdirFilter implements FilenameFilter {

		@Override
		public boolean accept(File dir, String name) {

			return (new File(dir, name).isDirectory());
		}
	}

	/**
	 * File search filter for file extensions
	 */
	public static class ExtFilter implements FilenameFilter {

		String ext;

		public ExtFilter(
				String ext) {

			this.ext = ext;
		}

		@Override
		public boolean accept(File dir, String name) {

			if (new File(dir, name).isDirectory())
				return false;

			name = name.toLowerCase();
			return name.endsWith(ext);
		}
	}

	/**
	 * File search filter working like file masks in console
	 */
	public static class MaskFilter implements FilenameFilter {

		String mask;

		public MaskFilter(
				String mask) {

			this.mask = mask.replace(".", "\\.");
			this.mask = this.mask.replace("?", ".");
			this.mask = this.mask.replace("*", ".*");
			this.mask = this.mask.toLowerCase();
		}

		@Override
		public boolean accept(File dir, String name) {

			if (new File(dir, name).isDirectory())
				return false;

			return name.toLowerCase().matches(mask);
		}
	}

	/**
	 * Filter for searching subdirectories working like file masks in console
	 */
	public static class DirMaskFilter extends MaskFilter {

		public DirMaskFilter(
				String mask) {
			super(mask);
		}

		@Override
		public boolean accept(File dir, String name) {

			if (!new File(dir, name).isDirectory())
				return false;

			return name.toLowerCase().matches(mask);
		}
	}

	/**
	 * Find all files in the given directory filtered by a filter class
	 * 
	 * @param dir
	 *            directory file object
	 * @param filter
	 *            filename filter (object of class derived from {@code FilenameFilter})
	 * 
	 * @return list of files
	 */
	public static List<File> findFilesInDir(File dir, FilenameFilter filter) {

		List<File> files = new ArrayList<>();

		if (dir.exists() && dir.isDirectory()) {

			// List filenames in directory matching filter class
			File[] fileArray = dir.listFiles(filter);
			if (fileArray != null) {

				// Build file objects from directory file object and file names
				files.addAll(Arrays.asList(fileArray));
			}
		}

		return files;
	}

	// Find all files in the given directory and its subdirectories filtered by a filter class
	private static void findFilesInTree(List<File> files, File dir, FilenameFilter filter) {

		// Add files in this directory
		files.addAll(findFilesInDir(dir, filter));

		// Recursively get files in subdirectories
		List<File> subdirs = findFilesInDir(dir, new SubdirFilter());
		for (File subdir : subdirs) {
			findFilesInTree(files, subdir, filter);
		}
	}

	/**
	 * Find all files in the given directory and its subdirectories filtered by a filter class
	 * 
	 * @param dir
	 *            directory file object
	 * @param filter
	 *            filename filter (object of class derived from {@code FilenameFilter})
	 * 
	 * @return list of files
	 */
	public static List<File> findFilesInTree(File dir, FilenameFilter filter) {

		List<File> files = new ArrayList<>();

		findFilesInTree(files, dir, filter);

		return files;
	}

	/**
	 * Find all files in the given directory filtered by a file name mask
	 * 
	 * @param dir
	 *            directory file object
	 * @param mask
	 *            file mask (with * and ?, like used in console programs)
	 * 
	 * @return list of files
	 */
	public static List<File> findFilesInDir(File dir, String mask) {

		return findFilesInDir(dir, new MaskFilter(mask));
	}

	/**
	 * Find (first) file in the given directory filtered by a file name mask
	 * 
	 * @param dir
	 *            directory file object
	 * @param mask
	 *            file mask (with * and ?, like used in console programs)
	 * 
	 * @return first file found or null
	 */
	public static File findFileInDir(File dir, String mask) {

		List<File> files = findFilesInDir(dir, new MaskFilter(mask));
		if (files.isEmpty())
			return null;
		else
			return files.get(0);
	}

	/**
	 * Find (first) subdirectory in the given directory filtered by a file name mask
	 * 
	 * @param dir
	 *            directory file object
	 * @param mask
	 *            file mask (with * and ?, like used in console programs)
	 * 
	 * @return first subdirectory found or null
	 */
	public static File findSubDir(File dir, String mask) {

		List<File> files = findFilesInDir(dir, new DirMaskFilter(mask));
		if (files.isEmpty())
			return null;
		else
			return files.get(0);
	}

	/**
	 * Find all files in the given directory and its subdirectories filtered by a file name mask
	 * 
	 * @param dir
	 *            directory file object
	 * @param mask
	 *            file mask (with * and ?, like used in console programs)
	 * 
	 * @return list of files
	 */
	public static List<File> findFilesInTree(File dir, String mask) {

		return findFilesInTree(dir, new MaskFilter(mask));
	}
}
