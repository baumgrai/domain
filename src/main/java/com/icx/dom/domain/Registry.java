package com.icx.dom.domain;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.common.CBase;
import com.icx.dom.common.Reflection;
import com.icx.dom.domain.DomainAnnotations.Accumulation;
import com.icx.dom.domain.DomainAnnotations.Removed;
import com.icx.dom.domain.DomainAnnotations.UseDataHorizon;
import com.icx.dom.domain.GuavaReplacements.ClassInfo;
import com.icx.dom.domain.GuavaReplacements.ClassPath;
import com.icx.dom.domain.sql.SqlDomainObject;

/**
 * Singleton to find and register domain classes. Only used internally.
 * 
 * @author RainerBaumgärtel
 */
public abstract class Registry extends Reflection {

	static final Logger log = LoggerFactory.getLogger(Registry.class);

	// Registered entities for any domain class
	static class DomainClassInfo {

		Constructor<? extends DomainObject> constructor;

		List<Field> dataFields = new ArrayList<>();
		List<Field> referenceFields = new ArrayList<>();
		List<Field> complexFields = new ArrayList<>();
	}

	// -------------------------------------------------------------------------
	// Static members
	// -------------------------------------------------------------------------

	// Properties
	public static Class<? extends DomainObject> baseClass = DomainObject.class; // Domain controller specific base class from which all domain objects must be derived

	// Domain classes
	private static List<Class<? extends DomainObject>> orderedDomainClasses = new ArrayList<>(); // List of registered domain classes in order of dependencies (if no circular references exists)
	private static Set<Class<? extends DomainObject>> removedDomainClasses = new HashSet<>(); // Set of domain classes which were removed in any version

	// Registry maps
	private static Map<Class<? extends DomainObject>, DomainClassInfo> domainClassInfoMap = new HashMap<>();
	private static Map<Class<? extends DomainObject>, Map<String, Field>> fieldByNameMap = new HashMap<>();
	private static Map<Field, Field> accumulationByReferenceFieldMap = new HashMap<>();

	// Temporarily used objects
	private static List<Field> preregisteredAccumulations = new ArrayList<>();
	private static List<Class<? extends DomainObject>> objectDomainClassesToRegister = new ArrayList<>(); // List of domain classes to register (including inherited classes)
	private static Set<Class<? extends DomainObject>> domainClassesDuringRegistration = new HashSet<>();

	// -------------------------------------------------------------------------
	// Domain classes (do not use these methods during Registration!)
	// -------------------------------------------------------------------------

	// Get registered domain classes
	public static List<Class<? extends DomainObject>> getRegisteredDomainClasses() { // Do not use duringRegistration!
		return new ArrayList<>(orderedDomainClasses);
	}

	// Get loaded (non-abstract) object domain classes
	public static List<Class<? extends DomainObject>> getRegisteredObjectDomainClasses() { // Do not use during Registration!
		return orderedDomainClasses.stream().filter(c -> !Modifier.isAbstract(c.getModifiers())).collect(Collectors.toList());
	}

	public static List<Class<? extends DomainObject>> getRelevantDomainClasses() {

		List<Class<? extends DomainObject>> relevantDomainClasses = new ArrayList<>();
		relevantDomainClasses.addAll(orderedDomainClasses);
		relevantDomainClasses.addAll(removedDomainClasses);

		return relevantDomainClasses;
	}

	// Check if class is domain class
	public static boolean isRegisteredDomainClass(Class<?> cls) { // Do not use during Registration!
		return orderedDomainClasses.contains(cls);
	}

	// Check if class is object domain class (top level of inheritance)
	public static boolean isObjectDomainClass(Class<? extends DomainObject> domainClass) {
		return (!Modifier.isAbstract(domainClass.getModifiers()));
	}

	// Check if class is base (or only) domain class of objects
	public static boolean isBaseDomainClass(Class<? extends DomainObject> domainClass) {
		return (domainClass.getSuperclass() == baseClass);
	}

	// Get declaring class of field
	@SuppressWarnings("unchecked")
	public static <T> T getDeclaringDomainClass(Field field) {
		return (T) field.getDeclaringClass();
	}

	// Get superclass of domain class
	@SuppressWarnings("unchecked")
	public static <T> T getReferencedDomainClass(Field refField) {
		return (T) refField.getType();
	}

	// Get superclass of domain class
	@SuppressWarnings("unchecked")
	public static <T> T getSuperclass(Class<? extends DomainObject> domainClass) {
		return (T) domainClass.getSuperclass();
	}

	// -------------------------------------------------------------------------
	// Inheritance
	// -------------------------------------------------------------------------

	// Build stack of base domain classes inherited from object domain class including object domain class itself (e.g. Bianchi -> [ Bike, Racebike, Bianchi]
	public static Stack<Class<? extends DomainObject>> getInheritanceStack(Class<? extends DomainObject> c) {

		Stack<Class<? extends DomainObject>> objectClasses = new Stack<>();

		do {
			if (baseClass.isAssignableFrom(c)) {
				objectClasses.add(0, c); // object domain class is top level in stack (retrieved by first call of pop(), iteration order in contrast is from bottom to top!)
				c = getSuperclass(c);
			}
		} while (c != baseClass && baseClass.isAssignableFrom(c));

		return objectClasses;
	}

	// Check if domain class or any of its inherited domain classes is 'data horizon' controlled (has annotation @UseDataHorizon)
	public static boolean isDataHorizonControlled(Class<? extends DomainObject> domainClass) {
		return getInheritanceStack(domainClass).stream().anyMatch(c -> c.isAnnotationPresent(UseDataHorizon.class));
	}

	// -------------------------------------------------------------------------
	// Fields
	// -------------------------------------------------------------------------

	// Allowed types
	private static boolean isBooleanType(Class<?> cls) {
		return (cls == Boolean.class || cls == boolean.class);
	}

	private static boolean isNumberType(Class<?> cls) {
		return (cls == int.class || cls == Integer.class || cls == long.class || cls == Long.class || cls == double.class || cls == Double.class || cls == BigInteger.class || cls == BigDecimal.class);
	}

	// Check if field is of one of the types which forces to interpret field as 'data' field
	private static boolean isDataFieldType(Class<?> type) {

		return (String.class.isAssignableFrom(type) || isBooleanType(type) || isNumberType(type) || Enum.class.isAssignableFrom(type) || LocalDateTime.class.isAssignableFrom(type)
				|| LocalDate.class.isAssignableFrom(type) || LocalTime.class.isAssignableFrom(type) || File.class.isAssignableFrom(type) || byte[].class.isAssignableFrom(type));
	}

	// Data field -> column
	public static boolean isDataField(Field field) {
		return isDataFieldType(field.getType());
	}

	// Reference field -> column with foreign key constraint
	public static boolean isReferenceField(Field field) {
		return baseClass.isAssignableFrom(field.getType());
	}

	// Set, List or Map field -> entry table (check also for 'hidden' accumulations missing @Accumulation annotation)
	public static boolean isComplexField(Field field) {

		if (!(field.getGenericType() instanceof ParameterizedType)) {
			return false;
		}

		Type type1 = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];

		return (Map.class.isAssignableFrom(field.getType())
				|| Collection.class.isAssignableFrom(field.getType()) && !isAccumulationField(field) && !(type1 instanceof Class<?> && baseClass.isAssignableFrom((Class<?>) type1)));
	}

	// Accumulation field (no SQL association)
	public static boolean isAccumulationField(Field field) {
		return field.isAnnotationPresent(Accumulation.class);
	}

	// Get field by name without using Reflection - check base domain classes of given domain class
	public static Field getFieldByName(Class<? extends DomainObject> domainClass, String fieldName) {

		Field field = null;
		for (Class<? extends DomainObject> dc : Registry.getInheritanceStack(domainClass)) {

			field = fieldByNameMap.get(dc).get(fieldName);
			if (field != null) {
				break;
			}
		}

		return field;
	}

	// Get constructor for domain class
	@SuppressWarnings("unchecked")
	public static <T extends DomainObject> Constructor<T> getConstructor(Class<T> domainClass) {
		return (Constructor<T>) domainClassInfoMap.get(domainClass).constructor;
	}

	// Get registered data fields for domain class in definition order
	public static List<Field> getDataFields(Class<? extends DomainObject> domainClass) {
		return domainClassInfoMap.get(domainClass).dataFields;
	}

	// Get fields of domain class referencing objects of same or other domain class in definition order
	public static List<Field> getReferenceFields(Class<? extends DomainObject> domainClass) {
		return domainClassInfoMap.get(domainClass).referenceFields;
	}

	// Get fields which are related to a table containing elements of a collection of entries of a key/value map
	// Note: All Collection or Map fields which are are not annotated with @SaveAsString or @Accumulation are table related fields
	public static List<Field> getComplexFields(Class<? extends DomainObject> domainClass) {
		return domainClassInfoMap.get(domainClass).complexFields;
	}

	// Get data and reference fields of domain class
	public static List<Field> getDataAndReferenceFields(Class<? extends DomainObject> domainClass) {

		List<Field> dataAndReferenceFields = new ArrayList<>();
		dataAndReferenceFields.addAll(getDataFields(domainClass));
		dataAndReferenceFields.addAll(getReferenceFields(domainClass));

		return dataAndReferenceFields;
	}

	// Get all registered fields of domain class
	public static List<Field> getRegisteredFields(Class<? extends DomainObject> domainClass) {

		List<Field> allFields = new ArrayList<>();
		allFields.addAll(getDataFields(domainClass));
		allFields.addAll(getReferenceFields(domainClass));
		allFields.addAll(getComplexFields(domainClass));

		return allFields;
	}

	// Get fields relevant for registration of domain class
	public static List<Field> getRelevantFields(Class<? extends DomainObject> domainClass) {
		return Stream.of(domainClass.getDeclaredFields()).filter(f -> !Modifier.isStatic(f.getModifiers()) && !Modifier.isTransient(f.getModifiers()) && !f.isAnnotationPresent(Accumulation.class))
				.collect(Collectors.toList());
	}

	// Get accumulation fields of domain class
	public static List<Field> getAccumulationFields(Class<? extends DomainObject> domainClass) {
		return accumulationByReferenceFieldMap.values().stream().filter(f -> f.getDeclaringClass() == domainClass).collect(Collectors.toList());
	}

	// Get accumulation by reference field
	public static Field getAccumulationFieldForReferenceField(Field referenceField) {
		return accumulationByReferenceFieldMap.get(referenceField);
	}

	// Get reference fields of any domain classes referencing this object domain class or inherited domain classes
	public static List<Field> getAllReferencingFields(Class<? extends DomainObject> domainObjectClass) {
		return orderedDomainClasses.stream().flatMap(c -> getReferenceFields(c).stream()).filter(f -> f.getType().isAssignableFrom(domainObjectClass)).collect(Collectors.toList());
	}

	// -------------------------------------------------------------------------
	// Find and register domain classes
	// -------------------------------------------------------------------------

	// Register data, reference and table related fields to serialize
	// Do not use isDomainClass() and isObjectDomainClass() here because not all domain classes are already registered in this state!
	private static void registerDomainClass(Class<? extends DomainObject> domainClass) throws DomainException {

		String className = (domainClass.isMemberClass() ? domainClass.getDeclaringClass().getSimpleName() + "$" : "") + domainClass.getSimpleName();
		String comment = (Modifier.isAbstract(domainClass.getModifiers()) ? " { \t// inherited class" : domainClass.isMemberClass() ? " { \t// inner object class" : " { \t// object class");

		log.info("REG: \tclass {} {}", className, comment);

		// Initialize reflection maps for domain class
		fieldByNameMap.put(domainClass, new HashMap<>());
		domainClassInfoMap.put(domainClass, new DomainClassInfo());

		// Register default constructor
		try {
			domainClassInfoMap.get(domainClass).constructor = domainClass.getConstructor();
		}
		catch (NoSuchMethodException nsmex) {
			throw new DomainException("Parameterless default constructor does not exist for domain class '" + domainClass.getSimpleName()
					+ "'! If specific constructors are defined also the parameterless default constructor must be defined!");
		}

		// Log standard fields of DomainObject class for all domain classes
		try {
			log.info("REG:\t\t{}; // system", fieldDeclaration(DomainObject.class.getDeclaredField("id")));
			if (SqlDomainObject.class.isAssignableFrom(domainClass)) {
				log.info("REG:\t\t{}; // system", fieldDeclaration(SqlDomainObject.class.getDeclaredField("lastModifiedInDb")));
			}
		}
		catch (NoSuchFieldException e) {
			throw new DomainException("Class 'DomainObject' does not contain id field!");
		}

		// Register serializable fields of this domain class in order of definition
		for (Field field : domainClass.getDeclaredFields()) {

			// Do not serialize static or transient fields
			if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
				continue;
			}

			// Do not register removed fields
			Removed removed = field.getAnnotation(Removed.class);
			if (removed != null && removed.version().length() > 0) {
				log.info("REG:\t\tField '{}' was removed in version {}", field.getName(), removed.version());
				continue;
			}

			// Allow access to field using Reflection
			field.setAccessible(true);

			// Register field by name
			fieldByNameMap.get(domainClass).put(field.getName(), field);

			// Register field depending on type

			Class<?> type = field.getType();
			String fieldDeclaration = fieldDeclaration(field).replaceAll("\\w+\\#", ""); // Log simple field type excluding declaring class prefix
			String qualifiedName = qualifiedName(field);
			String deprecated = (field.isAnnotationPresent(Deprecated.class) ? " (deprecated)" : "");

			if (isDataField(field)) {

				// Simple data field
				domainClassInfoMap.get(domainClass).dataFields.add(field);
				log.info("REG:\t\t{}; \t// data field{}", fieldDeclaration, deprecated);
			}
			else if (isReferenceField(field)) {

				// Reference field
				domainClassInfoMap.get(domainClass).referenceFields.add(field);
				String innerClass = (Arrays.asList(domainClass.getDeclaredClasses()).contains(type) ? "to inner class object " : "");
				log.info("REG:\t\t{}; \t// reference{}{}", fieldDeclaration, innerClass, deprecated);
			}
			else if (isAccumulationField(field)) {

				// Accumulation field (pre-register)
				preregisteredAccumulations.add(field);
				log.info("REG:\t\t{}; \t// accumulation{}", fieldDeclaration, deprecated);
			}
			else if (isComplexField(field)) {
				if (Collection.class.isAssignableFrom(type)) {

					// Table related field for List or Set (separate SQL table)
					domainClassInfoMap.get(domainClass).complexFields.add(field);
					log.info("REG:\t\t{}; \t// table related field for {} of elements{}", fieldDeclaration, type.getSimpleName().toLowerCase(), deprecated);
				}
				else if (Map.class.isAssignableFrom(type)) {

					// Table related field for Map (separate SQL table)
					domainClassInfoMap.get(domainClass).complexFields.add(field);
					log.info("REG:\t\t{}; \t// table related field for key/value map{}", fieldDeclaration, deprecated);
				}
			}
			else {
				log.error("REG:\tCannot serialize field '{}' of type '{}'!", qualifiedName, type);
				if (Set.class.isAssignableFrom(field.getType())) {
					Type type1 = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
					if (type1 instanceof Class<?> && baseClass.isAssignableFrom((Class<?>) type1)) {
						log.warn("REG: Accumulation fields must be annotated with @Acccumulation!");
					}
				}
			}
		}

		log.info("REG: \t}");
	}

	// Remove field from registry
	public static void unregisterField(Field field) {

		Class<? extends DomainObject> domainClass = getDeclaringDomainClass(field);
		String fieldDeclaration = fieldDeclaration(field);

		if (domainClassInfoMap.get(domainClass).dataFields.contains(field)) {

			domainClassInfoMap.get(domainClass).dataFields.remove(field);
			log.info("REG: Unregistered data field: {}", fieldDeclaration);
		}
		else if (domainClassInfoMap.get(domainClass).referenceFields.contains(field)) {

			domainClassInfoMap.get(domainClass).referenceFields.remove(field);
			log.info("REG: Unregistered reference field: {}", fieldDeclaration);
		}
		else if (domainClassInfoMap.get(domainClass).complexFields.contains(field)) {

			domainClassInfoMap.get(domainClass).complexFields.remove(field);
			log.info("REG: Unregistered element set or key/value map field: {}", fieldDeclaration);
		}
		else {
			log.error("REG: Field {} was not registered!", fieldDeclaration);
		}
	}

	// Register accumulation fields for domain class
	@SuppressWarnings("unchecked")
	private static void registerAccumulationFields() throws DomainException {

		log.info("REG: Register accumulations:");

		for (Field accumulationField : preregisteredAccumulations) {

			Class<? extends DomainObject> domainClassOfAccumulatedObjects = (Class<? extends DomainObject>) ((ParameterizedType) accumulationField.getGenericType()).getActualTypeArguments()[0];
			Field refFieldForAccumulation = null;

			// Find reference field for accumulation
			boolean fromAnnotation = false;
			String refFieldName = (accumulationField.isAnnotationPresent(Accumulation.class) ? accumulationField.getAnnotation(Accumulation.class).refField() : "");
			if (!CBase.isEmpty(refFieldName)) {

				// Get reference field from accumulation field annotation
				Class<? extends DomainObject> domainClassWhereReferenceFieldIsDefined = null;
				if (refFieldName.contains(".")) {

					// Domain class defining reference field is explicitly defined (typically cross reference class for many-to-many relation)
					String domainClassName = CBase.untilFirst(refFieldName, ".");
					refFieldName = CBase.behindFirst(refFieldName, ".");

					domainClassWhereReferenceFieldIsDefined = orderedDomainClasses.stream().filter(c -> c.getSimpleName().equals(domainClassName)).findFirst().orElse(null);
					if (domainClassWhereReferenceFieldIsDefined == null) {
						throw new DomainException("'" + domainClassName + "' got from annotation of accumulation field '" + qualifiedName(accumulationField) + "' a is not a registered domain class!");
					}
				}
				else {
					domainClassWhereReferenceFieldIsDefined = domainClassOfAccumulatedObjects;
				}

				try {
					refFieldForAccumulation = domainClassWhereReferenceFieldIsDefined.getDeclaredField(refFieldName);
				}
				catch (NoSuchFieldException e) {
					throw new DomainException("Reference field '" + refFieldName + "' got from annotation of accumulation field '" + qualifiedName(accumulationField) + "' does not exist in '"
							+ domainClassWhereReferenceFieldIsDefined.getSimpleName() + "'");
				}

				fromAnnotation = true;
			}
			else {
				// Try to find one and only reference field in accumulated objects class referencing class where accumulation is defined
				List<Field> possibleRefFieldsForAccumulation = Arrays.asList(domainClassOfAccumulatedObjects.getDeclaredFields()).stream()
						.filter(f -> f.getType().isAssignableFrom(accumulationField.getDeclaringClass())).collect(Collectors.toList());

				if (possibleRefFieldsForAccumulation.isEmpty()) {
					throw new DomainException("No reference field for '" + accumulationField.getDeclaringClass().getSimpleName() + "' found in '" + domainClassOfAccumulatedObjects.getSimpleName()
							+ "' for accumulation field '" + qualifiedName(accumulationField) + "'!");
				}
				else if (possibleRefFieldsForAccumulation.size() > 1) {
					throw new DomainException("More than one possible reference field for '" + accumulationField.getDeclaringClass().getSimpleName() + "' found in '"
							+ domainClassOfAccumulatedObjects.getSimpleName() + "' for accumulation field '" + qualifiedName(accumulationField) + "': "
							+ possibleRefFieldsForAccumulation.parallelStream().map(Field::getName).collect(Collectors.toList())
							+ "! Please specify reference field for accumulation using @Accumulation annotation and 'refField' attribute");
				}
				else {
					refFieldForAccumulation = possibleRefFieldsForAccumulation.get(0);
					if (accumulationByReferenceFieldMap.containsKey(refFieldForAccumulation)) {
						throw new DomainException(
								"More than one accumulation field found for '" + domainClassOfAccumulatedObjects.getSimpleName() + "' in '" + accumulationField.getDeclaringClass().getSimpleName()
										+ "': '" + accumulationByReferenceFieldMap.get(refFieldForAccumulation).getName() + "' and '" + accumulationField.getName() + "'!");
					}
				}
			}

			// Register accumulation for reference field
			if (refFieldForAccumulation != null) {
				accumulationByReferenceFieldMap.put(refFieldForAccumulation, accumulationField);
				log.info("REG: \t'{}' for reference field: '{}' {}", qualifiedName(accumulationField), qualifiedName(refFieldForAccumulation), (fromAnnotation ? " (from annotation)" : ""));
			}
		}
	}

	// Register domain class recursively considering dependencies
	@SuppressWarnings("unchecked")
	private static void registerDomainClassRecursive(Class<? extends DomainObject> domainClass) throws DomainException {

		// Avoid multiple registration of domain classes and infinite loops on circular references
		if (domainClassesDuringRegistration.contains(domainClass)) {
			return;
		}
		domainClassesDuringRegistration.add(domainClass);

		// Do not register removed domain classes
		Removed removed = domainClass.getAnnotation(Removed.class);
		if (removed != null && removed.version().length() > 0) {
			log.info("REG:\t\tDomain class '{}' was removed in version {}", domainClass.getSimpleName(), removed.version());
			removedDomainClasses.add(domainClass);
			return;
		}

		// Register base domain class before domain class itself
		if (getSuperclass(domainClass) != baseClass) {
			registerDomainClassRecursive(getSuperclass(domainClass));
		}

		// Register parent domain classes before child domain class
		for (Field field : Stream.of(domainClass.getDeclaredFields()).filter(f -> isReferenceField(f)).collect(Collectors.toList())) {

			Class<? extends DomainObject> referencedDomainClass = getReferencedDomainClass(field);

			if (Registry.isDataHorizonControlled(referencedDomainClass) && !Registry.isDataHorizonControlled(domainClass)) {
				log.warn(
						"REG: Domain class '{}' is not data horizon controlled (annotation @UseDataHorizon) but parent class '{}' is! This suppresses unregistering '{}'-objects which are out of data horizon if they have '{}' childeren. Please check if this behaviour is intended!",
						domainClass.getSimpleName(), referencedDomainClass.getSimpleName(), referencedDomainClass.getSimpleName(), domainClass.getSimpleName());
			}

			registerDomainClassRecursive(referencedDomainClass);
		}

		// Register inner domain classes before domain class itself
		for (Class<?> innerClass : domainClass.getDeclaredClasses()) {

			if (baseClass.isAssignableFrom(innerClass)) {

				if (Modifier.isStatic(innerClass.getModifiers())) {
					registerDomainClassRecursive((Class<? extends DomainObject>) innerClass);
				}
				else {
					throw new DomainException("Inner domain class '" + innerClass.getName() + "' must be declared as static!");
				}
			}
		}

		// Register domain class itself
		registerDomainClass(domainClass);

		// Add domain class (in order of dependencies)
		orderedDomainClasses.add(domainClass);
	}

	// Set base class and clear all registration related collections and maps
	private static void init(Class<? extends DomainObject> baseClass) {

		Registry.baseClass = baseClass;

		orderedDomainClasses.clear();
		domainClassInfoMap.clear();
		fieldByNameMap.clear();
		accumulationByReferenceFieldMap.clear();
		preregisteredAccumulations.clear();
		objectDomainClassesToRegister.clear();
		domainClassesDuringRegistration.clear();
	}

	// Check if all domain classes which are no object domain classes are not instantiable (abstract)
	private static void checkObjectDomainClasses() throws DomainException {

		for (Class<? extends DomainObject> objectDomainClass : objectDomainClassesToRegister) {

			Stack<Class<? extends DomainObject>> baseDomainClasses = getInheritanceStack(objectDomainClass);
			baseDomainClasses.pop();

			for (Class<? extends DomainObject> baseDomainClass : baseDomainClasses) {

				if (objectDomainClassesToRegister.contains(baseDomainClass)) {
					throw new DomainException("Domain class '" + baseDomainClass.getName()
							+ "' is base class of one of the object domain classes and therefore cannot be an 'object' domain class itself and must be declared as 'abstract' (instantiable 'object' domain classes must be top level of inheritance hirarchie)");
				}
			}
		}

	}

	// Register domain classes in specified package
	@SuppressWarnings("unchecked")
	public static void registerDomainClasses(Class<? extends DomainObject> baseClass, String domainPackageName) throws DomainException {

		// Reset all lists and maps for registration
		init(baseClass);

		// Find all object domain classes in given package and sub packages
		for (ClassInfo classinfo : ClassPath.from(Thread.currentThread().getContextClassLoader()).getTopLevelClassesRecursive(domainPackageName)) {
			try {
				Class<?> cls = Class.forName(classinfo.getName());

				if (baseClass.isAssignableFrom(cls) && isObjectDomainClass((Class<? extends DomainObject>) cls)) {
					objectDomainClassesToRegister.add((Class<? extends DomainObject>) cls);
				}
			}
			catch (ClassNotFoundException e) {
				throw new DomainException("Class '" + classinfo.getName() + "' found as Domain class cannot be loaded");
			}
		}

		log.info("REG: Object domain classes in package {}: {}", domainPackageName, objectDomainClassesToRegister.stream().map(Class::getSimpleName).collect(Collectors.toList()));

		// Check if all domain classes which are no object domain classes are not instantiable (abstract)
		checkObjectDomainClasses();

		// Register all domain classes (recursively in order of dependencies)
		log.info("REG: Register domain classes:");
		for (Class<? extends DomainObject> objectDomainClass : objectDomainClassesToRegister) {
			registerDomainClassRecursive(objectDomainClass);
		}

		// Register accumulation fields
		registerAccumulationFields();
	}

	// Register specified domain classes
	@SafeVarargs
	public static final void registerDomainClasses(Class<? extends DomainObject> baseClass, Class<? extends DomainObject>... domainClasses) throws DomainException {

		// Reset all lists and maps for registration
		init(baseClass);

		// Select object domain classes from given domain classes
		objectDomainClassesToRegister.addAll(Stream.of(domainClasses).filter(dc -> isObjectDomainClass(dc)).collect(Collectors.toList()));

		log.info("REG: Object domain classes to register: {}", objectDomainClassesToRegister.stream().map(Class::getSimpleName).collect(Collectors.toList()));

		// Check if all domain classes which are no object domain classes are not instantiable (abstract)
		checkObjectDomainClasses();

		// Register all domain classes (recursively in order of dependencies)
		log.info("REG: Register domain classes:");
		for (Class<? extends DomainObject> objectDomainClass : objectDomainClassesToRegister) {
			registerDomainClassRecursive(objectDomainClass);
		}

		// Register accumulation fields
		registerAccumulationFields();
	}

	@SuppressWarnings("unchecked")
	private static boolean determineCircularReferences(Class<? extends DomainObject> domainClass, Set<List<String>> circularReferences, Stack<Class<? extends DomainObject>> stack) {

		if (stack.contains(domainClass)) {

			List<String> circularReference = stack.subList(stack.indexOf(domainClass), stack.size()).stream().map(dc -> dc.getSimpleName()).collect(Collectors.toList());
			circularReferences.add(circularReference);

			return true;
		}

		stack.push(domainClass);

		for (Field field : Stream.of(domainClass.getDeclaredFields()).filter(f -> isReferenceField(f)).collect(Collectors.toList())) {

			Class<? extends DomainObject> referencedDomainClass = (Class<? extends DomainObject>) field.getType();

			determineCircularReferences(referencedDomainClass, circularReferences, stack);
		}

		stack.pop();

		return false;
	}

	private static boolean listsEqualIgnoreOrder(List<String> l1, List<String> l2) {
		return (l1 == null && l2 == null || l1 != null && l2 != null && l1.size() == l2.size() && l1.containsAll(l2) && l2.containsAll(l1));
	}

	public static Set<List<String>> determineCircularReferences() {

		Set<List<String>> rawCircularReferences = new HashSet<>();

		for (Class<? extends DomainObject> domainClass : objectDomainClassesToRegister) {
			determineCircularReferences(domainClass, rawCircularReferences, new Stack<>());
		}

		Set<List<String>> circularReferences = new HashSet<>();

		for (List<String> circularReference : rawCircularReferences) {
			if (circularReferences.stream().noneMatch(cr -> listsEqualIgnoreOrder(cr, circularReference))) {
				circularReferences.add(circularReference);
			}
		}

		return circularReferences;
	}

}
