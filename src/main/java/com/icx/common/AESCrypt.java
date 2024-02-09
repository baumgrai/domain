package com.icx.common;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
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

	// TODO: Support file encryption

	public static SecretKey generateKey(int n) throws NoSuchAlgorithmException {

		KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
		keyGenerator.init(n);

		return keyGenerator.generateKey();
	}

	public static SecretKey getKeyFromPassword(String password, String salt) throws NoSuchAlgorithmException, InvalidKeySpecException {

		SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
		KeySpec spec = new PBEKeySpec(password.toCharArray(), salt.getBytes(), 65536, 256);

		return new SecretKeySpec(factory.generateSecret(spec).getEncoded(), "AES");
	}

	public static IvParameterSpec generateIv() {

		byte[] iv = new byte[16];
		new SecureRandom().nextBytes(iv);

		return new IvParameterSpec(iv);
	}

	public static String encrypt(String algorithm, String input, SecretKey key, IvParameterSpec iv) throws Exception {

		Cipher cipher = Cipher.getInstance(algorithm);
		cipher.init(Cipher.ENCRYPT_MODE, key, iv);
		byte[] cipherText = cipher.doFinal(input.getBytes());

		return Base64.getEncoder().encodeToString(cipherText);
	}

	public static String decrypt(String algorithm, String cipherText, SecretKey key, IvParameterSpec iv) throws Exception {

		Cipher cipher = Cipher.getInstance(algorithm);
		cipher.init(Cipher.DECRYPT_MODE, key, iv);
		byte[] plainText = cipher.doFinal(Base64.getDecoder().decode(cipherText));

		return new String(plainText);
	}

	public static void main(String[] args) throws Exception {

		String input = "abcdefghijklmnopqrstuvwxyz";
		// SecretKey key = generateKey(128);
		SecretKey key = getKeyFromPassword("XYZ", "1234567890");
		IvParameterSpec ivParameterSpec = generateIv();
		String algorithm = "AES/CBC/PKCS5Padding";
		String cipherText = encrypt(algorithm, input, key, ivParameterSpec);
		String plainText = decrypt(algorithm, cipherText, key, ivParameterSpec);
		System.out.println(input);
		System.out.println(cipherText);
		System.out.println(plainText);
		System.out.println();

		for (int i = 0; i < 3; i++) {
			key = generateKey(128);
			System.out.println(Common.byteArrayToHexString(key.getEncoded()));
		}
		System.out.println();

		for (int i = 0; i < 3; i++) {
			ivParameterSpec = generateIv();
			System.out.println(Common.byteArrayToHexString(ivParameterSpec.getIV()));
		}
	}
}
