package com.icx.common;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * AES encryption and decryption - from Baeldung
 */
public abstract class AESCrypt {

	private static final String ALGORITHM = "AES/CBC/PKCS5Padding";

	/**
	 * Generates encryption key from password and 'salt'.
	 * 
	 * @param password
	 *            password
	 * @param salt
	 *            salt
	 * 
	 * @return secret key
	 * 
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeySpecException
	 */
	public static SecretKey getKeyFromPassword(String password, String salt) throws NoSuchAlgorithmException, InvalidKeySpecException {

		SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
		KeySpec spec = new PBEKeySpec(password.toCharArray(), salt.getBytes(), 65536, 256);

		return new SecretKeySpec(factory.generateSecret(spec).getEncoded(), "AES");
	}

	/**
	 * Encrypt a string using specified algorithm and (generated) encryption key.
	 * 
	 * @param algorithm
	 *            algorithm specification (e.g.: "AES/CBC/PKCS5Padding")
	 * @param input
	 *            string to encrypt
	 * @param key
	 *            encryption key
	 * 
	 * @return encrypted string
	 * 
	 * @throws NoSuchPaddingException
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidAlgorithmParameterException
	 * @throws InvalidKeyException
	 * @throws BadPaddingException
	 * @throws IllegalBlockSizeException
	 */
	public static String encrypt(String algorithm, String input, SecretKey key)
			throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException {

		byte[] iv = new byte[16];
		new SecureRandom().nextBytes(iv);
		Cipher cipher = Cipher.getInstance(algorithm);
		cipher.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
		byte[] cipherText = cipher.doFinal(input.getBytes());

		return Common.byteArrayToHexString(iv) + Common.byteArrayToHexString(cipherText);
	}

	/**
	 * Encrypt a string using "AES/CBC/PKCS5Padding" algorithm and password and 'salt'.
	 * 
	 * @param input
	 *            string to encrypt
	 * @param cryptPassword
	 *            encryption password
	 * @param cryptSalt
	 *            encryption 'salt'
	 * 
	 * @return encrypted string
	 * 
	 * @throws InvalidKeySpecException
	 * @throws BadPaddingException
	 * @throws IllegalBlockSizeException
	 * @throws InvalidAlgorithmParameterException
	 * @throws NoSuchPaddingException
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 */
	public static String encrypt(String input, String cryptPassword, String cryptSalt)
			throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, InvalidKeySpecException {

		return encrypt(ALGORITHM, input, getKeyFromPassword(cryptPassword, cryptSalt));
	}

	/**
	 * Decrypt an encrypted string using specified algorithm and (generated) encryption key.
	 * 
	 * @param algorithm
	 *            algorithm specification (e.g.: "AES/CBC/PKCS5Padding")
	 * @param cipherText
	 *            encrypted string
	 * @param key
	 *            encryption key
	 * 
	 * @return original string
	 * 
	 * @throws NoSuchPaddingException
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidAlgorithmParameterException
	 * @throws InvalidKeyException
	 * @throws BadPaddingException
	 * @throws IllegalBlockSizeException
	 */
	public static String decrypt(String algorithm, String cipherText, SecretKey key)
			throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException {

		Cipher cipher = Cipher.getInstance(algorithm);
		cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(Common.hexStringToByteArray(cipherText.substring(0, 32))));
		byte[] plainText = cipher.doFinal(Common.hexStringToByteArray(cipherText.substring(32)));

		return new String(plainText);
	}

	/**
	 * Decrypt an encrypted string using "AES/CBC/PKCS5Padding" algorithm and password and 'salt'.
	 * 
	 * @param cipherText
	 *            encrypted string
	 * @param cryptPassword
	 *            encryption password
	 * @param cryptSalt
	 *            encryption 'salt'
	 * 
	 * @return original string
	 * 
	 * @throws InvalidKeySpecException
	 * @throws NoSuchPaddingException
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidAlgorithmParameterException
	 * @throws InvalidKeyException
	 * @throws BadPaddingException
	 * @throws IllegalBlockSizeException
	 */
	public static String decrypt(String cipherText, String cryptPassword, String cryptSalt)
			throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException, InvalidKeySpecException {
		return decrypt(ALGORITHM, cipherText, getKeyFromPassword(cryptPassword, cryptSalt));
	}

	// public static void main(String[] args) throws Exception {
	//
	// String input = "abcdefghijklmnopqrstuvwxyz";
	// // SecretKey key = generateKey(128);
	// SecretKey key = getKeyFromPassword("XYZ", "1234567890");
	// String cipherText = encrypt(ALGORITHM, input, key);
	// String plainText = decrypt(ALGORITHM, cipherText, key);
	// System.out.println(input);
	// System.out.println(cipherText);
	// System.out.println(plainText);
	// System.out.println();
	//
	// System.out.println(input);
	// System.out.println(decrypt(encrypt(input, "ABCDEFGH", "12345678"), "ABCDEFGH", "12345678"));
	// System.out.println();
	// }
}
