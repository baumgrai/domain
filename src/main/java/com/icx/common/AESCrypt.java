package com.icx.common;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import com.icx.common.base.Common;

/**
 * AES encryption and decryption - from Baeldung
 */
public class AESCrypt {

	private static final String ALGORITHM = "AES/CBC/PKCS5Padding";

	// TODO: Support file encryption

	public static SecretKey getKeyFromPassword(String password, String salt) throws NoSuchAlgorithmException, InvalidKeySpecException {

		SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
		KeySpec spec = new PBEKeySpec(password.toCharArray(), salt.getBytes(), 65536, 256);

		return new SecretKeySpec(factory.generateSecret(spec).getEncoded(), "AES");
	}

	public static String encrypt(String algorithm, String input, SecretKey key) throws Exception {

		byte[] iv = new byte[16];
		new SecureRandom().nextBytes(iv);
		Cipher cipher = Cipher.getInstance(algorithm);
		cipher.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
		byte[] cipherText = cipher.doFinal(input.getBytes());

		return Common.byteArrayToHexString(iv) + Common.byteArrayToHexString(cipherText);
	}

	public static String encrypt(String input, String cryptPassword, String cryptSalt) throws Exception {
		return encrypt(ALGORITHM, input, getKeyFromPassword(cryptPassword, cryptSalt));
	}

	public static String decrypt(String algorithm, String cipherText, SecretKey key) throws Exception {

		Cipher cipher = Cipher.getInstance(algorithm);
		cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(Common.hexStringToByteArray(cipherText.substring(0, 32))));
		byte[] plainText = cipher.doFinal(Common.hexStringToByteArray(cipherText.substring(32)));

		return new String(plainText);
	}

	public static String decrypt(String cipher, String cryptPassword, String cryptSalt) throws Exception {
		return decrypt(ALGORITHM, cipher, getKeyFromPassword(cryptPassword, cryptSalt));
	}

	public static void main(String[] args) throws Exception {

		String input = "abcdefghijklmnopqrstuvwxyz";
		// SecretKey key = generateKey(128);
		SecretKey key = getKeyFromPassword("XYZ", "1234567890");
		String algorithm = "AES/CBC/PKCS5Padding";
		String cipherText = encrypt(algorithm, input, key);
		String plainText = decrypt(algorithm, cipherText, key);
		System.out.println(input);
		System.out.println(cipherText);
		System.out.println(plainText);
		System.out.println();

		System.out.println(input);
		System.out.println(decrypt(encrypt(input, "ABCDEFGH", "12345678"), "ABCDEFGH", "12345678"));
		System.out.println();

	}
}
