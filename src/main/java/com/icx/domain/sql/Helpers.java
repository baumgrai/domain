package com.icx.domain.sql;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.CFile;
import com.icx.common.Common;

/**
 * General helpers
 * 
 * @author baumgrai
 */
public abstract class Helpers extends Common {

	static final Logger log = LoggerFactory.getLogger(Helpers.class);

	// Count new and changed objects grouped by object domain classes (for logging only)
	static <T extends SqlDomainObject> Set<Entry<String, Integer>> groupCountsByDomainClassName(Set<T> objects) {

		return objects.stream().collect(Collectors.groupingBy(Object::getClass)).entrySet().stream().map(e -> new SimpleEntry<>(e.getKey().getSimpleName(), e.getValue().size()))
				.collect(Collectors.toSet());
	}

	// Build string lists with a maximum of max elements (Oracle limit for # of elements in WHERE IN (...) clause = 1000)
	static List<String> buildStringLists(Set<?> elements, int max) {

		List<String> stringLists = new ArrayList<>();
		if (elements == null || elements.isEmpty()) {
			return stringLists;
		}

		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (Object element : elements) {

			if (i % max != 0) {
				sb.append(",");
			}

			if (element instanceof String || element instanceof Enum || element instanceof Boolean || element instanceof File) {
				sb.append("'" + element + "'");
			}
			else {
				sb.append(element);
			}

			if (i % max == max - 1) {
				stringLists.add(sb.toString());
				sb.setLength(0);
			}

			i++;
		}

		if (sb.length() > 0) {
			stringLists.add(sb.toString());
		}

		return stringLists;
	}

	// Build up byte array containing file path and file content . If file cannot be read store only file path in database and set file content to an error message.
	public static byte[] buildFileByteEntry(File file, String columnName) {

		if (file == null) {
			log.warn("SDC: File to store is null");
			return new byte[0];
		}

		byte[] pathBytes = file.getAbsolutePath().getBytes(StandardCharsets.UTF_8);
		byte[] contentBytes;
		try {
			contentBytes = CFile.readBinary(file);
		}
		catch (IOException ioex) {
			log.warn("SDC: File '{}' cannot be read! Therefore column '{}' will be set to file path name but file itself contains an error message - {}", file, columnName, ioex.getMessage());
			contentBytes = "File did not exist or could not be read on storing to database!".getBytes(StandardCharsets.UTF_8);
		}
		byte[] entryBytes = new byte[2 + pathBytes.length + contentBytes.length];

		entryBytes[0] = (byte) (pathBytes.length / 0x100);
		entryBytes[1] = (byte) (pathBytes.length % 0x100);

		int b = 2;
		for (; b < pathBytes.length + 2; b++) {
			entryBytes[b] = pathBytes[b - 2];
		}

		for (; b < contentBytes.length + pathBytes.length + 2; b++) {
			entryBytes[b] = contentBytes[b - pathBytes.length - 2];
		}

		if (log.isDebugEnabled()) {
			log.debug("SDC: Store file '{}' containing {} bytes", file, contentBytes.length);
		}

		return entryBytes;
	}

	// Get file path length from binary coded file entry
	public static int getPathLength(byte[] entryBytes) {

		if (entryBytes == null) {
			return -1;
		}

		int pathLength = 0x100 * entryBytes[0] + entryBytes[1];

		if (log.isTraceEnabled()) {
			log.trace("SDC: File path length: {}", pathLength);
		}

		return pathLength;
	}

	// Get empty file object from binary coded file entry
	public static File getFile(byte[] entryBytes, int pathLength) {

		if (entryBytes == null) {
			return null;
		}

		int b = 2;
		byte[] pathBytes = new byte[pathLength];
		for (; b < pathLength + 2; b++) {
			pathBytes[b - 2] = entryBytes[b];
		}

		String filePathName = new String(pathBytes, StandardCharsets.UTF_8);

		if (log.isDebugEnabled()) {
			log.debug("SDC: File path: '{}'", filePathName);
		}

		return new File(filePathName);
	}

	// Get binary file content from binary coded file entry
	public static byte[] getFileContent(byte[] entryBytes, int pathLength) {

		if (entryBytes == null) {
			return new byte[0];
		}

		int b = 2 + pathLength;
		byte[] contentBytes = new byte[entryBytes.length - pathLength - 2];
		for (; b < entryBytes.length; b++) {
			contentBytes[b - pathLength - 2] = entryBytes[b];
		}

		if (log.isDebugEnabled()) {
			log.debug("SDC: File content length: {}", contentBytes.length);
		}

		return contentBytes;
	}

	// Rebuild file object from binary coded file entry
	public static File rebuildFile(byte[] entryBytes) throws IOException {

		if (entryBytes == null || entryBytes.length == 0) {
			log.warn("SDC: File entry is null or empty!");
			return null;
		}

		int pathLength = getPathLength(entryBytes);
		if (pathLength < 1 || pathLength > 1024) {
			log.warn("SDC: File entry is invalid! (length of file path is not in the range of 1-1024) - set file to null!");
			return null;
		}
		File file = getFile(entryBytes, pathLength);
		byte[] contentBytes = getFileContent(entryBytes, pathLength);

		CFile.writeBinary(file, contentBytes);

		return file;
	}

}
