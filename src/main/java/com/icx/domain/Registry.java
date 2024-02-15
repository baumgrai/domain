package com.icx.domain;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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

import com.icx.common.Reflection;
import com.icx.domain.DomainAnnotations.Accumulation;
import com.icx.domain.DomainAnnotations.Removed;
import com.icx.domain.DomainAnnotations.StoreAsString;
import com.icx.domain.DomainAnnotations.UseDataHorizon;
import com.icx.domain.GuavaReplacements.ClassInfo;
import com.icx.domain.GuavaReplacements.ClassPath;
import com.icx.domain.sql.SqlDomainObject;
import com.icx.domain.sql.tools.Java2Sql;
import com.icx.jdbc.SqlDbHelpers;

/**
 * Find and register domain classes. Only used internally.
 * <p>
 * Used by {@link Java2Sql} tool to generate SQL scripts for generating persistence database and also used during initialization of domain controller.
 * 
 * @param <T>
 *            type of domain object which will be registered ({@link SqlDomainObject}
 * 
 * @author baumgrai
 */
public class Registry<T extends DomainObject> extends Reflection {

	static final Logger log = LoggerFactory.getLogger(Registry.class);

	// -------------------------------------------------------------------------
	// Inner classes
	// -------------------------------------------------------------------------

	// Registered entities for any domain class
	class DomainClassInfo {

		Constructor<T> constructor;

		List<Field> dataFields = new ArrayList<>();
		List<Field> referenceFields = new ArrayList<>();
		List<Field> complexFields = new ArrayList<>();
	}

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	// Properties
	public Class<T> baseClass = null; // Domain controller specific base class from which all domain objects must be derived

	// Domain classes
	private List<Class<? extends T>> orderedDomainClasses = new ArrayList<>(); // List of registered domain classes in (half) order of dependencies (if no circular references exists)
	private Set<Class<? extends T>> removedDomainClasses = new HashSet<>(); // Set of domain classes which were removed in any version

	// Registry maps
	private Map<Class<? extends T>, DomainClassInfo> domainClassInfoMap = new HashMap<>();
	private Map<Class<? extends T>, Map<String, Field>> fieldByNameMap = new HashMap<>();
	private Map<Field, Field> accumulationByReferenceFieldMap = new HashMap<>();

	// Temporarily used objects
	private List<Field> preregisteredAccumulations = new ArrayList<>();
	private List<Class<? extends T>> objectDomainClassesToRegister = new ArrayList<>(); // List of domain classes to register (including inherited classes)
	private Set<Class<? extends T>> domainClassesDuringRegistration = new HashSet<>();

	// -------------------------------------------------------------------------
	// Domain classes (do not use these methods during Registration!)
	// -------------------------------------------------------------------------

	// Get registered domain classes
	public final List<Class<? extends T>> getRegisteredDomainClasses() { // Do not use duringRegistration!
		return orderedDomainClasses;
	}

	// Get loaded (non-abstract) object domain classes
	public List<Class<? extends T>> getRegisteredObjectDomainClasses() { // Do not use during Registration!
		return orderedDomainClasses.stream().filter(dc -> !Modifier.isAbstract(dc.getModifiers())).collect(Collectors.toList());
	}

	public List<Class<? extends T>> getRelevantDomainClasses() {

		List<Class<? extends T>> relevantDomainClasses = new ArrayList<>();
		orderedDomainClasses.forEach(relevantDomainClasses::add);
		removedDomainClasses.forEach(relevantDomainClasses::add);

		return relevantDomainClasses;
	}

	// Check if class is domain class
	public boolean isRegisteredDomainClass(Class<?> cls) { // Do not use during Registration!
		return orderedDomainClasses.contains(cls);
	}

	// Check if class is object domain class (top level of inheritance)
	public boolean isObjectDomainClass(Class<? extends T> domainClass) {
		return (!Modifier.isAbstract(domainClass.getModifiers()));
	}

	// Check if class is base (or only) domain class of objects
	public boolean isBaseDomainClass(Class<? extends T> domainClass) {
		return (domainClass.getSuperclass() == baseClass);
	}

	// -------------------------------------------------------------------------
	// Unchecked casts
	// -------------------------------------------------------------------------

	// Cast class to domain class
	@SuppressWarnings("unchecked")
	public Class<? extends T> castDomainClass(Class<?> cls) {
		return (Class<? extends T>) cls;
	}

	// Cast constructor to constructor of base class
	@SuppressWarnings("unchecked")
	public <S extends T> Constructor<T> castConstructor(Constructor<S> constructor) {
		return (Constructor<T>) constructor;
	}

	// Get class of object
	@SuppressWarnings("unchecked")
	public <S extends T> Class<S> getCastedDomainClass(DomainObject obj) {
		return (Class<S>) obj.getClass();
	}

	// Get declaring class of field
	@SuppressWarnings("unchecked")
	public <S extends T> Class<S> getCastedDeclaringDomainClass(Field field) {
		return (Class<S>) field.getDeclaringClass();
	}

	// Get referenced domain class of reference field
	@SuppressWarnings("unchecked")
	public <S extends T> Class<S> getCastedReferencedDomainClass(Field refField) {
		return (Class<S>) refField.getType();
	}

	// Get superclass of domain class
	@SuppressWarnings("unchecked")
	public <S extends T> Class<S> getCastedSuperclass(Class<S> domainClass) {
		return (Class<S>) domainClass.getSuperclass();
	}

	// -------------------------------------------------------------------------
	// Inheritance
	// -------------------------------------------------------------------------

	// Build stack of base domain classes where object domain class is inherited from including object domain class itself (e.g. Bianchi -> [ Bike, Racebike, Bianchi ]
	public List<Class<? extends T>> getDomainClassesFor(Class<? extends T> c) {

		List<Class<? extends T>> domainClasses = new Stack<>();
		do {
			if (baseClass.isAssignableFrom(c)) {
				domainClasses.add(0, c); // List starts with bottom-most domain class
				c = getCastedSuperclass(c);
			}
		} while (c != baseClass && baseClass.isAssignableFrom(c));
		return domainClasses;
	}

	// Check if domain class or any of its inherited domain classes is 'data horizon' controlled (has annotation @UseDataHorizon)
	public boolean isDataHorizonControlled(Class<? extends T> domainClass) {
		return getDomainClassesFor(domainClass).stream().anyMatch(c -> c.isAnnotationPresent(UseDataHorizon.class));
	}

	// -------------------------------------------------------------------------
	// Fields
	// -------------------------------------------------------------------------

	// Check if field is of one of the types which forces to interpret field as 'data' field
	private static boolean isDataFieldType(Class<?> cls) {
		return (SqlDbHelpers.isBasicType(cls) && !(cls == byte.class || cls == Byte.class || cls == float.class || cls == Float.class) || cls == byte[].class || cls == Byte[].class);
	}

	// Data field -> column
	public static boolean isDataField(Field field) {
		return (isDataFieldType(field.getType()) || field.isAnnotationPresent(StoreAsString.class));
	}

	// Reference field -> column with foreign key constraint
	public boolean isReferenceField(Field field) {
		return baseClass.isAssignableFrom(field.getType());
	}

	// Array, Set, List or Map field -> entry table (check also for 'hidden' accumulations missing @Accumulation annotation)
	public boolean isComplexField(Field field) {

		if (field.getType().isArray() && field.getType().getComponentType() != byte.class && field.getType().getComponentType() != Byte.class) {
			return true;
		}
		else if (field.getGenericType() instanceof ParameterizedType) {
			Type type1 = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
			return (Map.class.isAssignableFrom(field.getType())
					|| Collection.class.isAssignableFrom(field.getType()) && !isAccumulationField(field) && !(type1 instanceof Class<?> && baseClass.isAssignableFrom((Class<?>) type1)));
		}
		else {
			return false;
		}
	}

	// Accumulation field (no SQL association)
	public static boolean isAccumulationField(Field field) {
		return field.isAnnotationPresent(Accumulation.class);
	}

	// Get field by name without using Reflection - check base domain classes of given domain class
	public <S extends T> Field getFieldByName(Class<S> domainClass, String fieldName) {

		Field field = null;
		for (Class<? extends T> dc : getDomainClassesFor(domainClass)) {
			field = fieldByNameMap.get(dc).get(fieldName);
			if (field != null) {
				break;
			}
		}
		return field;
	}

	// Get constructor for domain class
	public Constructor<T> getConstructor(Class<? extends T> domainClass) {
		return domainClassInfoMap.get(domainClass).constructor;
	}

	// Get registered data fields for domain class in definition order
	public List<Field> getDataFields(Class<? extends T> domainClass) {
		return domainClassInfoMap.get(domainClass).dataFields;
	}

	// Get fields of domain class referencing objects of same or other domain class in definition order
	public List<Field> getReferenceFields(Class<? extends T> domainClass) {
		return domainClassInfoMap.get(domainClass).referenceFields;
	}

	// Get fields which are related to a table containing elements of a collection of entries of a key/value map
	// Note: All Collection or Map fields which are are not annotated with @SaveAsString or @Accumulation are table related fields
	public List<Field> getComplexFields(Class<? extends T> domainClass) {
		return domainClassInfoMap.get(domainClass).complexFields;
	}

	// Get data and reference fields of domain class
	public List<Field> getDataAndReferenceFields(Class<? extends T> domainClass) {

		List<Field> dataAndReferenceFields = new ArrayList<>();
		dataAndReferenceFields.addAll(getDataFields(domainClass));
		dataAndReferenceFields.addAll(getReferenceFields(domainClass));
		return dataAndReferenceFields;
	}

	// Get all registered fields of domain class
	public List<Field> getRegisteredFields(Class<? extends T> domainClass) {

		List<Field> allFields = new ArrayList<>();
		allFields.addAll(getDataFields(domainClass));
		allFields.addAll(getReferenceFields(domainClass));
		allFields.addAll(getComplexFields(domainClass));
		return allFields;
	}

	// Get fields relevant for registration of domain class
	public List<Field> getRelevantFields(Class<? extends T> domainClass) {
		return Stream.of(domainClass.getDeclaredFields()).filter(f -> !Modifier.isStatic(f.getModifiers()) && !Modifier.isTransient(f.getModifiers()) && !f.isAnnotationPresent(Accumulation.class))
				.collect(Collectors.toList());
	}

	// Get accumulation fields of domain class
	public List<Field> getAccumulationFields(Class<? extends T> domainClass) {
		return accumulationByReferenceFieldMap.values().stream().filter(f -> f.getDeclaringClass() == domainClass).collect(Collectors.toList());
	}

	// Get accumulation by reference field
	public Field getAccumulationFieldForReferenceField(Field referenceField) {
		return accumulationByReferenceFieldMap.get(referenceField);
	}

	// Get reference fields of any domain classes referencing this object domain class or inherited domain classes
	public List<Field> getAllReferencingFields(Class<? extends T> domainObjectClass) {
		return orderedDomainClasses.stream().flatMap(c -> getReferenceFields(c).stream()).filter(f -> f.getType().isAssignableFrom(domainObjectClass)).collect(Collectors.toList());
	}

	// -------------------------------------------------------------------------
	// Find and register domain classes
	// -------------------------------------------------------------------------

	// Register data, reference and table related fields to serialize
	// Note: do not use isDomainClass() and isObjectDomainClass() here because not all domain classes are already registered in this state!
	private <S extends T> void registerDomainClass(Class<S> domainClass) throws DomainException {

		String className = (domainClass.isMemberClass() ? domainClass.getDeclaringClass().getSimpleName() + "$" : "") + domainClass.getSimpleName();
		String comment = (Modifier.isAbstract(domainClass.getModifiers()) ? " { \t// inherited class" : domainClass.isMemberClass() ? " { \t// inner object class" : " { \t// object class");

		log.info("REG: \tclass {} {}", className, comment);

		// Initialize reflection maps for domain class
		fieldByNameMap.put(domainClass, new HashMap<>());
		domainClassInfoMap.put(domainClass, new DomainClassInfo());

		// Register default constructor
		try {
			domainClassInfoMap.get(domainClass).constructor = castConstructor(domainClass.getConstructor());
		}
		catch (NoSuchMethodException nsmex) {
			throw new DomainException("Parameterless default constructor does not exist for domain class '" + domainClass.getSimpleName()
					+ "'! If specific constructors are defined also the parameterless default constructor must be defined!");
		}

		// Check if parameterless default constructor does not throw any exception
		Class<?>[] exceptionTypes = domainClassInfoMap.get(domainClass).constructor.getExceptionTypes();
		if (exceptionTypes.length > 0) {
			throw new DomainException("Parameterless default constructor for domain class '" + domainClass.getSimpleName() + "' throws " + exceptionTypes
					+ "! Parameterless default constructors of domain classes may not throw exceptions");
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
				if (type.isArray() || Collection.class.isAssignableFrom(type)) {

					// Table related field for array, List or Set (separate SQL table)
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
				log.error("REG: Cannot serialize field '{}' of type '{}'!", qualifiedName, type);
				if (Set.class.isAssignableFrom(type)) {
					Type type1 = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
					if (type1 instanceof Class<?> && baseClass.isAssignableFrom((Class<?>) type1)) {
						log.warn("REG: Accumulation fields must be annotated with @Acccumulation!");
					}
				}
				else if (Float.class.isAssignableFrom(type)) {
					throw new DomainException("float and Float fields are not supported! Please use double or Double instaed of float/Float!");
				}
				else if (Byte.class.isAssignableFrom(type)) {
					throw new DomainException("byte and Byte fields are not supported! Please use short or Short instaed of byte/Byte!");
				}
			}
		}

		log.info("REG: \t}");
	}

	// Remove field from registry
	public void unregisterField(Field field) {

		Class<? extends T> domainClass = getCastedDeclaringDomainClass(field);
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
			log.info("REG: Unregistered collection/map field: {}", fieldDeclaration);
		}
		else {
			log.error("REG: Field {} was not registered!", fieldDeclaration);
		}
	}

	// Register accumulation fields for domain class
	private void registerAccumulationFields() throws DomainException {

		log.info("REG: Register accumulations:");

		for (Field accumulationField : preregisteredAccumulations) {

			Class<? extends T> domainClassOfAccumulatedObjects = castDomainClass((Class<?>) ((ParameterizedType) accumulationField.getGenericType()).getActualTypeArguments()[0]);
			Field refFieldForAccumulation = null;

			// Find reference field for accumulation
			boolean fromAnnotation = false;
			String refFieldName = (accumulationField.isAnnotationPresent(Accumulation.class) ? accumulationField.getAnnotation(Accumulation.class).refField() : "");
			if (!isEmpty(refFieldName)) {

				// Get reference field from accumulation field annotation
				Class<? extends T> domainClassWhereReferenceFieldIsDefined = null;
				if (refFieldName.contains(".")) {

					// Domain class defining reference field is explicitly defined (typically cross reference class for many-to-many relation)
					String domainClassName = untilFirst(refFieldName, ".");
					refFieldName = behindFirst(refFieldName, ".");
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
	private void registerDomainClassRecursive(Class<? extends T> domainClass) throws DomainException {

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
		if (getCastedSuperclass(domainClass) != baseClass) {
			registerDomainClassRecursive(getCastedSuperclass(domainClass));
		}

		// Register parent domain classes before child domain class
		for (Field field : Stream.of(domainClass.getDeclaredFields()).filter(this::isReferenceField).collect(Collectors.toList())) {

			Class<? extends T> referencedDomainClass = getCastedReferencedDomainClass(field);
			if (isDataHorizonControlled(referencedDomainClass) && !isDataHorizonControlled(domainClass)) {
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
					registerDomainClassRecursive(castDomainClass(innerClass));
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
	private void init(Class<T> baseClass) {

		this.baseClass = baseClass;

		orderedDomainClasses.clear();
		domainClassInfoMap.clear();
		fieldByNameMap.clear();
		accumulationByReferenceFieldMap.clear();
		preregisteredAccumulations.clear();
		objectDomainClassesToRegister.clear();
		domainClassesDuringRegistration.clear();
	}

	private void checkObjectDomainClass(Class<? extends T> objectDomainClass) throws DomainException {

		List<Class<? extends T>> domainClasses = getDomainClassesFor(objectDomainClass);

		for (Class<? extends T> domainClass : domainClasses) {
			if (domainClass != objectDomainClass && objectDomainClassesToRegister.contains(domainClass)) {
				throw new DomainException("Domain class '" + ((Class<?>) domainClass).getName()
						+ "' is base class of one of the object domain classes and therefore cannot be an 'object' domain class itself and must be declared as 'abstract' (instantiable 'object' domain classes must be top level of inheritance hirarchie)");
			}
		}
	}

	// Register domain classes in specified package
	public void registerDomainClasses(Class<T> baseClass, String domainPackageName) throws DomainException {

		// Reset all lists and maps for registration
		init(baseClass);

		// Find all object domain classes in given package and sub packages
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		for (ClassInfo classinfo : ClassPath.from(cl).getTopLevelClassesRecursive(domainPackageName)) {
			try {
				Class<? extends T> cls = castDomainClass(Class.forName(classinfo.getName()));
				if (baseClass.isAssignableFrom(cls) && isObjectDomainClass(cls)) {
					objectDomainClassesToRegister.add(cls);
				}
			}
			catch (ClassNotFoundException e) {
				throw new DomainException("Class '" + classinfo.getName() + "' found as Domain class cannot be loaded");
			}
		}

		log.info("REG: Object domain classes in package {}: {}", domainPackageName, objectDomainClassesToRegister.stream().map(Class::getSimpleName).collect(Collectors.toList()));

		// Check if all domain classes which are no object domain classes are not instantiable (abstract)
		for (Class<? extends T> objectDomainClass : objectDomainClassesToRegister) {
			checkObjectDomainClass(objectDomainClass);
		}

		// Register all domain classes (recursively in order of dependencies)
		log.info("REG: Register domain classes:");
		for (Class<? extends T> objectDomainClass : objectDomainClassesToRegister) {
			registerDomainClassRecursive(objectDomainClass);
		}

		// Register accumulation fields
		registerAccumulationFields();
	}

	// Register specified domain classes
	@SafeVarargs
	public final void registerDomainClasses(Class<T> baseClass, Class<? extends T>... domainClasses) throws DomainException {

		// Reset all lists and maps for registration
		init(baseClass);

		// Select object domain classes from given domain classes
		objectDomainClassesToRegister.addAll(Stream.of(domainClasses).filter(this::isObjectDomainClass).collect(Collectors.toList()));
		log.info("REG: Object domain classes to register: {}", objectDomainClassesToRegister.stream().map(Class::getSimpleName).collect(Collectors.toList()));

		// Check if all domain classes which are not object domain classes are not instantiable (abstract)
		for (Class<? extends T> objectDomainClass : objectDomainClassesToRegister) {
			checkObjectDomainClass(objectDomainClass);
		}

		// Register all domain classes (recursively in order of dependencies)
		log.info("REG: Register domain classes:");
		for (Class<? extends T> objectDomainClass : objectDomainClassesToRegister) {
			registerDomainClassRecursive(objectDomainClass);
		}

		// Register accumulation fields
		registerAccumulationFields();
	}

	// Check for circular references involving given domain class
	private void determineCircularReferences(Class<? extends T> domainClass, Set<List<String>> circularReferences, Stack<Class<? extends T>> stack) {

		if (stack.contains(domainClass)) {
			List<String> circularReference = stack.subList(stack.indexOf(domainClass), stack.size()).stream().map(Class::getSimpleName).collect(Collectors.toList());
			circularReferences.add(circularReference);
			return;
		}
		stack.push(domainClass);

		for (Field field : Stream.of(domainClass.getDeclaredFields()).filter(this::isReferenceField).collect(Collectors.toList())) {
			Class<? extends T> referencedDomainClass = getCastedReferencedDomainClass(field);
			determineCircularReferences(referencedDomainClass, circularReferences, stack);
		}
		stack.pop();
	}

	// Check if lists equal ignoring order of elements
	private static boolean listsEqualIgnoreOrder(List<String> l1, List<String> l2) {
		return (l1 == null && l2 == null || l1 != null && l2 != null && l1.size() == l2.size() && l1.containsAll(l2) && l2.containsAll(l1));
	}

	// Check for circular references within registered domain classes
	public Set<List<String>> determineCircularReferences() {

		// Determine circular references
		Set<List<String>> rawCircularReferences = new HashSet<>();
		for (Class<? extends T> domainClass : objectDomainClassesToRegister) {
			determineCircularReferences(domainClass, rawCircularReferences, new Stack<>());
		}

		// Remove doublets circular references from found
		Set<List<String>> circularReferences = new HashSet<>();
		for (List<String> circularReference : rawCircularReferences) {
			if (circularReferences.stream().noneMatch(cr -> listsEqualIgnoreOrder(cr, circularReference))) {
				circularReferences.add(circularReference);
			}
		}
		return circularReferences;
	}
}
