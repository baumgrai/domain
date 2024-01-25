package com.icx.dom.app.bikestore.domain.client;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.MDC;

import com.icx.common.base.CList;
import com.icx.common.base.CMap;
import com.icx.common.base.CResource;
import com.icx.common.base.Common;
import com.icx.dom.app.bikestore.BikeStoreApp;
import com.icx.dom.app.bikestore.BikeStoreApp.Counters;
import com.icx.dom.app.bikestore.domain.bike.Bike;
import com.icx.dom.app.bikestore.domain.bike.CityBike;
import com.icx.dom.app.bikestore.domain.bike.MTB;
import com.icx.dom.app.bikestore.domain.bike.RaceBike;
import com.icx.dom.domain.DomainAnnotations.Accumulation;
import com.icx.dom.domain.DomainAnnotations.SqlColumn;
import com.icx.dom.domain.DomainAnnotations.SqlTable;
import com.icx.dom.domain.sql.SqlDomainObject;

/**
 * Clients of different countries ordering bikes if different types.
 * 
 * @author baumgrai
 */
@SqlTable(uniqueConstraints = { "firstName, country" }, indexes = "bikeSize") // Define multi column UNIQUE constraints and indexes here
public class Client extends SqlDomainObject {

	// Types

	public enum Gender {
		MALE, FEMALE, DIVERSE
	}

	// Note: 'Country' and 'Region' enums must be defined here (and not in inner 'RegionInUse' class) because doing otherwise leads to Java 8 compiler error
	public enum Country {
		ALGERIA, ARGENTINA, AUSTRALIA, AUSTRIA, BANGLADESH, BELGIUM, BRAZIL, CANADA, CHILE, CHINA, COLOMBIA, CUBA, DENMARK, ECUADOR, EGYPT, ENGLAND, ETHIOPIA, FINLAND, FRANCE, GERMANY, GHANA, GREECE,
		HONG_KONG, HUNGARY, INDIA, INDONESIA, IRAN, IRAQ, IRELAND, ISRAEL, ITALY, JAMAICA, JAPAN, JORDAN, KAZAKHSTAN, KENYA, SOUTH_KOREA, LIBYA, LUXEMBOURG, MALAYSIA, MEXICO, MOROCCO, NEPAL,
		NETHERLANDS, NIGERIA, NORWAY, OMAN, PAKISTAN, PERU, PHILIPPINES, POLAND, PORTUGAL, QATAR, ROMANIA, RUSSIA, SAUDI_ARABIA, SCOTTLAND, SINGAPORE, SOUTH_AFRICA, SPAIN, SUDAN, SWEDEN, SWITZERLAND,
		TAIWAN, TANZANIA, THAILAND, TURKEY, UGANDA, UKRAINE, UNITED_ARAB_EMIRATES, UNITED_STATES, VENEZUELA, VIETNAM, WALES, YEMEN, ZAMBIA, NEW_ZEALAND
	}

	public enum Region {
		AFRICA, AMERICA_AUSTRALIA, MIDDLE_ASIA, EAST_ASIA, EAST_EUROPE, WEST_EUROPE
	}

	// Inner classes

	public static class RegionInProgress extends SqlDomainObject { // Domain object class may be an inner class (but only on first level)

		//@formatter:off
		private static final Map<Region, List<Country>> regionCountryMap = CMap.newMap(
				Region.AFRICA, CList.newList(Country.ALGERIA, Country.EGYPT, Country.ETHIOPIA, Country.GHANA, Country.KENYA, Country.LIBYA, Country.MOROCCO, Country.NIGERIA, Country.SUDAN, Country.SOUTH_AFRICA, Country.TANZANIA, Country.UGANDA, Country.ZAMBIA),
				Region.AMERICA_AUSTRALIA, CList.newList(Country.ARGENTINA, Country.BRAZIL, Country.CHILE, Country.COLOMBIA, Country.CUBA, Country.ECUADOR, Country.JAMAICA, Country.MEXICO, Country.PERU,  Country.VENEZUELA, Country.CANADA, Country.UNITED_STATES, Country.AUSTRALIA, Country.NEW_ZEALAND),
				Region.MIDDLE_ASIA, CList.newList(Country.IRAN, Country.IRAQ, Country.ISRAEL, Country.JORDAN, Country.KAZAKHSTAN, Country.NEPAL,Country.OMAN, Country.PAKISTAN, Country.QATAR, Country.SAUDI_ARABIA, Country.TURKEY, Country.UNITED_ARAB_EMIRATES, Country.YEMEN),
				Region.EAST_ASIA, CList.newList(Country.BANGLADESH, Country.CHINA, Country.HONG_KONG, Country.INDIA, Country.INDONESIA, Country.JAPAN, Country.SOUTH_KOREA, Country.MALAYSIA, Country.PHILIPPINES, Country.SINGAPORE, Country.TAIWAN, Country.THAILAND, Country.VIETNAM),
				Region.EAST_EUROPE, CList.newList(Country.AUSTRIA, Country.FINLAND, Country.GERMANY, Country.ITALY, Country.GREECE, Country.HUNGARY, Country.NORWAY, Country.POLAND, Country.ROMANIA, Country.RUSSIA, Country.SWEDEN, Country.UKRAINE),
				Region.WEST_EUROPE, CList.newList(Country.BELGIUM, Country.NETHERLANDS, Country.DENMARK, Country.ENGLAND, Country.FRANCE, Country.IRELAND, Country.LUXEMBOURG, Country.PORTUGAL, Country.SCOTTLAND, Country.SPAIN, Country.SWITZERLAND, Country.WALES));
		//@formatter:on

		public static Map<Region, List<Country>> getRegionCountryMap() {
			return regionCountryMap;
		}

		public static Region getRegion(Country country) {

			for (Entry<Region, List<Country>> entry : regionCountryMap.entrySet()) {
				if (entry.getValue().contains(country)) {
					return entry.getKey();
				}
			}

			return null;
		}

		// Members

		@SqlColumn(unique = true)
		public Region region;

		// Test...

		public static void main(String[] args) {
			System.out.println(Country.values().length);
			System.out.println(regionCountryMap.values().stream().flatMap(l -> l.stream()).count());
			for (Region region : Region.values()) {
				System.out.println(regionCountryMap.get(region).size());
			}
		}
	}

	// Statics

	//@formatter:off
	private static final Map<Country, List<String>> countryNamesMap = CMap.newMap(
			Country.ALGERIA, CList.newList("Nazim", "Khalil", "Samir", "Farid", "Abdou", "Yasmine", "Amira", "Meriem", "Imane", "Fatima"), 
			Country.ARGENTINA, CList.newList("Franco", "Juan", "Joaquín", "Mauro", "Fernando", "Camila", "Agustina", "Juana", "Victoria", "Ana"), 
			Country.AUSTRALIA, CList.newList("James", "Jack", "Liam", "Jake", "Dylan", "Emily", "Kate", "Georgia", "Grace", "Amy"),
			Country.AUSTRIA, CList.newList("Alexander", "Paul", "Adrian", "Max", "Jakob", "Anna", "Sophie", "Lisa", "Julia", "Laura"), 
			Country.BANGLADESH, CList.newList("Ahnaf", "Rafi", "Siam", "Abir", "Fardin", "Barsha", "Samiha", "Nazia", "Safina", "Rafia"), 
			Country.BELGIUM, CList.newList("Julien", "Maxime", "Alexandre", "Nicolas", "Robin", "Marie", "Manon", "Camille", "Chloé", "Justine"), 
			Country.BRAZIL, CList.newList("Gabriel", "João", "Guilherme", "Pedro", "Thiago", "Amanda", "Gabriela", "Beatriz", "Larissa", "Leticia"), 
			Country.CANADA, CList.newList("Alex", "William", "Simon", "Ryan", "Kevin", "Laurence", "Madison", "Hannah", "Catherine", "Rachel"), 
			Country.CHILE, CList.newList("Diego", "Felipe", "Pablo", "Cristobal", "Claudio", "Fernanda", "Isabel", "Constanza", "Javiera", "Francisca"),
			Country.CHINA, CList.newList("Chen", "Wang", "Zhang", "Liu", "Xu", "Li", "Sun", "Yu", "Zhou", "Yan"), 
			Country.COLOMBIA, CList.newList("Alejandro", "Jose", "Sergio", "Juán", "Andrés", "Angie", "Daniela", "Alejandra", "Karen", "Natalia"), 
			Country.CUBA, CList.newList("Rafael", "Carlos", "Alberto", "Luis", "Julio", "Veronica", "Samantha", "Irma", "Teresa", "Melissa"), 
			Country.DENMARK, CList.newList("Mads", "Frederik", "Asger", "Malthe", "Henrik", "Mille", "Rikke", "Laerke", "Astrid", "Mette"), 
			Country.ECUADOR, CList.newList("Santiago", "Edison", "Sebastián", "Antonio", "Darwin", "Jessica", "Viviana", "Cristina", "Lucía", "Adriana"), 
			Country.EGYPT, CList.newList("Mahmoud", "Karim", "Omar", "Youssef", "Mustafa", "Nour", "Farah", "Zainab", "Heba", "Jomana"), 
			Country.ENGLAND, CList.newList("William", "Tom", "Adam", "Joe", "Luke", "Jess", "Holly", "Lauren", "Caitlin", "Beth"), 
			Country.ETHIOPIA, CList.newList("Nebiyu", "Dawit", "Bezalel", "Solomon", "Natnael", "Saron", "Naomi", "Beza", "Nebi", "Fikir"), 
			Country.FINLAND, CList.newList("Veikko", "Antti", "Jami", "Jaakko", "Miika", "Anni", "Siiri", "Oona", "Vilma", "Veera"), 
			Country.FRANCE, CList.newList("Antoine", "Romain", "Quentin", "Jérémy", "Mathieu", "Mathilde", "Julie", "Pauline", "Juliette", "Céline"), 
			Country.GERMANY, CList.newList("Michael", "Horst", "Otto", "Egon", "Torsten", "Angelika", "Katrin", "Claudia", "Doris", "Elisabeth"), 
			Country.GHANA, CList.newList("Prince", "Ebenezer", "Alhassan", "Maxwell", "Emmanuel", "Abigail", "Maame", "Rashida", "Eunice", "Priscilla"), 
			Country.GREECE, CList.newList("Dimitris", "Kostas", "Spiros", "Stelios", "Giorgos", "Anastasia", "Athina", "Konstantina", "Ioanna", "Ariadne"), 
			Country.HONG_KONG, CList.newList("Wong", "Jason", "Lee", "Iam", "Chan", "Ma", "Chu", "Zoe", "Wei", "Wan"), 
			Country.HUNGARY, CList.newList("Ádám", "Attila", "Balázs", "Zsolt", "Gábor", "Eszter", "Réka", "Zsófi", "Virág", "Dóra"), 
			Country.INDIA, CList.newList("Neeraj", "Rahul", "Amit", "Deepak", "Rakesh", "Priya", "Ishita", "Aishwarya", "Shivangi", "Tanvi"), 
			Country.INDONESIA, CList.newList("Arief", "Farel", "Stanley", "Fadhlan", "Ahmad", "Putri", "Nurul", "Dewi", "Ayu", "Shinta"), 
			Country.IRAN, CList.newList("Ali", "Shayan", "Mehdi", "Saber", "Sajad", "Zahra", "Fatemeh", "Aida", "Niloofar", "Rozhan"), 
			Country.IRAQ, CList.newList("Muhammad", "Kassim", "Sami", "Fadhil", "Nasim", "Telenaz", "Nono", "Fatema", "Latoya", "Sawa"), 
			Country.IRELAND, CList.newList("Conor", "Sean", "Shane", "Rory", "Eoin", "Aoife", "Sinéad", "Gemma", "Róisín", "Shauna"), 
			Country.ISRAEL, CList.newList("Ben", "David", "Dan", "Samuel", "Reuven", "Noa", "Yuval", "Yahel", "Dor", "Sarah"), 
			Country.ITALY, CList.newList("Gianluca", "Fabio", "Giuseppe", "Matteo", "Giacomo", "Giulia", "Chiara", "Letizia", "Alessandra", "Paola"), 
			Country.JAMAICA, CList.newList("Ashani", "Saturn", "Damain", "Okieve", "Malique", "Azaria", "Selena", "Kyra", "Daveen", "Rhosanda"), 
			Country.JAPAN, CList.newList("Yuki", "Hiroki", "Takuya", "Kaito", "Yusuke", "Yuka", "Natsumi", "Miyu", "Saki", "Shiori"), 
			Country.JORDAN, CList.newList("Shaker", "Khaled", "Moha", "Hussien", "Yousef", "Hadeel", "Aqilah", "Bushra", "Beela", "Rawan"),
			Country.KAZAKHSTAN, CList.newList("Sultan", "Anuar", "Bekzat", "Kirill", "Naurizbek", "Aliya", "Dasha", "Zarina", "Moldir", "Aknur"), 
			Country.KENYA, CList.newList("Ndwiga", "Kioko", "Fadhili", "Obuya", "Mpenda", "Makena", "Zawadi", "Akinyi", "Wawira", "Kerubo"), 
			Country.SOUTH_KOREA, CList.newList("Kim", "Park", "Choi", "Jeong", "Hwang", "Minji", "Jiwon", "Yoon", "Jang", "Soyeon"), 
			Country.LIBYA, CList.newList("Bashir", "Jamal", "Tarek", "Said", "Hussein", "Nuri", "Reda", "Fatma", "Aisha", "Rania"), 
			Country.LUXEMBOURG, CList.newList("Pierre", "Eliot", "Ray", "Thierry", "Damien", "Madison", "Anaïs", "Annchen", "Lucie", "Janelle"), 
			Country.MALAYSIA, CList.newList("Afiq", "Haziq", "Amir", "Isfahann", "Ashraf", "Nur", "Aisyah", "Nurul", "Zulaikha", "Irdina"),
			Country.MEXICO, CList.newList("Angel", "Sergio", "Ricardo", "Arturo", "Jorge", "Andrea", "María", "Dulce", "Estefania", "Ximena"), 
			Country.MOROCCO, CList.newList("Rachid", "Mohamed", "Hakim", "Nizar", "Yasser", "Salma", "Aya", "Meryem", "Hafsa", "Zineb"), 
			Country.NEPAL, CList.newList("Krishna", "Udgam", "Ram", "Bishal", "Kamal", "Shambhav", "Yahnaa", "Mahima", "Sajita", "Kusum"), 
			Country.NETHERLANDS, CList.newList("Jan", "Pieter", "Hans", "Stijn", "Daan", "Antje", "Lotte", "Lieke", "Femke", "Meike"), 
			Country.NIGERIA, CList.newList("Kingsley", "Goodluck", "Obi", "Ebenezer", "Ngozi", "Olabisi", "Queen", "Abisoye", "Owoeye", "Sharon"), 
			Country.NORWAY, CList.newList("Osvald", "Hakon", "Einar", "Tore", "Leif", "Silje", "Runa", "Eir", "Dagmar", "Beret"),
			Country.OMAN, CList.newList("Newton", "Surya", "Abdulkhakiq", "Faisal", "Iqbal", "Tagucci", "Waaede", "Rahaf", "Ayah", "Eman"), 
			Country.PAKISTAN, CList.newList("Usman", "Bilal", "Raja", "Yasir", "Salman", "Ayesha", "Laiba", "Manahil", "Sadaf", "Fizza"), 
			Country.PERU, CList.newList("José", "Víctor", "Rodrigo", "Jesus", "Gustavo", "Milagros", "Luz", "Fiorella", "Valeria", "Lucia"), 
			Country.PHILIPPINES, CList.newList("Jacinto", "Bayani", "Agapito", "Crisanto", "Dakila", "Amihan", "Diwata", "Mayumi", "Hiraya", "Imelda"), 
			Country.POLAND, CList.newList("Bartek", "Wojciech", "Mateusz", "Piotr", "Paweł", "Katarzyna", "Agnieszka", "Martyna", "Ewa", "Zuza"), 
			Country.PORTUGAL, CList.newList("Vasco", "Miguel", "Afonso", "Gonçalo", "Duarte", "Inês", "Joana", "Carolina", "Margarida", "Andreia"),
			Country.QATAR, CList.newList("Laraib", "Hayat", "Kasun", "Husam", "Abdoulaye", "Ghalia", "Alysha", "Ammahrah", "Jewel", "Zalfaa"), 
			Country.ROMANIA, CList.newList("Bogdan", "Vlad", "Marcu", "Radu", "Dragos", "Ioana", "Roxana", "Mihaela", "Ramona", "Denisa"), 
			Country.RUSSIA, CList.newList("Vladimir", "Ivan", "Igor", "Sergey", "Sasha", "Olga", "Irina", "Natasha", "Ekaterina", "Svetlana"), 
			Country.SAUDI_ARABIA, CList.newList("Nasser", "Orimer", "Fares", "Asad", "Saiyyad", "Reem", "Noura", "Doha", "Raihana", "Haifa"), 
			Country.SCOTTLAND, CList.newList("Finlay", "Craig", "Lennox", "Malcolm", "Alastair", "Fiona", "Blair", "Nessa", "Ansley", "Aileen"), 
			Country.SINGAPORE, CList.newList("Aiden", "Elroy", "Aniq", "Cheejun", "Jiasheng", "Joyce", "Hazel", "Alyssa", "Scarlett", "Meredith"),
			Country.SOUTH_AFRICA, CList.newList("Brendan", "Thuthuka", "Ethan", "Calvin", "Lawrence", "Megan", "Tallulah", "Ammaarah", "Haajarah", "Ruby"), 
			Country.SPAIN, CList.newList("Álvaro", "Javier", "Ruben", "Iker", "Enrique", "Mònica", "Carmen", "Inés", "Raquel", "Angela"), 
			Country.SUDAN, CList.newList("Malse", "Kariem", "Garang", "Macquei", "Alghaliy", "Roaa", "Ruba", "Doaa", "Esraa", "Abrar"), 
			Country.SWEDEN, CList.newList("Lars", "Hakan", "Anders", "Johan", "Ingmar", "Ingrid", "Stina", "Elin", "Greta", "Tuva"), 
			Country.SWITZERLAND, CList.newList("Beat", "Roger", "Urs", "Ueli", "Moritz", "Anneli", "Dorli", "Regula", "Fränzi", "Gritli"), 
			Country.TAIWAN, CList.newList("Huang", "Eric", "Yang", "Leo", "Su", "Lin", "Judy", "Wu", "Hsu", "Chang"),
			Country.TANZANIA, CList.newList("Uromi", "Mbonea", "Jossal", "Opiyo", "Tawfq", "Neema", "Jackline", "Rehema", "Oprahnash", "Latifa"), 
			Country.THAILAND, CList.newList("Sarawut", "Tanawat", "Nattapong", "Sirichai", "Teerapat", "Ploy", "Sudarat", "Nan", "Fern", "Kanokwan"), 
			Country.TURKEY, CList.newList("Murat", "Mehmet", "Emre", "Furkan", "Bülent", "Zeynep", "Beyza", "Gamze", "Gülten", "Ezgi"), 
			Country.UGANDA, CList.newList("Moses", "Muhindo", "Mugerwa", "Godfrey", "Rubangakene", "Nakkazi", "Wanyana", "Hadijah", "Rwandarugari", "Nagadya"), 
			Country.UKRAINE, CList.newList("Misha", "Dimitri", "Dima", "Oleg", "Ruslan", "Nastia", "Alina", "Daria", "Oksana", "Polina"), 
			Country.UNITED_ARAB_EMIRATES, CList.newList("Mohammed", "Abdullah", "Rashid", "Ahmed", "Saeed", "Maryam", "Manar", "Nada", "Zaynab", "Jawaher"), 
			Country.UNITED_STATES, CList.newList("Tyler", "John", "Logan", "Brian", "Hunter", "Ashley", "Taylor", "Morgan", "Amber", "Haley"), 
			Country.VENEZUELA, CList.newList("Ernesto", "Mario", "Sebastian", "Mauricio", "Edgar", "Patricia", "Virginia", "Manuela", "Marian", "Clau"), 
			Country.VIETNAM, CList.newList("Nguyen", "Duc", "Viet", "Phuc", "Ngoc", "Linh", "Anh", "Trang", "Nhung", "Nga"), 
			Country.WALES, CList.newList("Gareth", "Owen", "Siôn", "Rhodri", "Gethin", "Rhiannon", "Sioned", "Lowri", "Gwen", "Erin"), 
			Country.YEMEN, CList.newList("Saleh", "Abdo", "Akram", "Nabil", "Anwar", "Wafa", "Amal", "Jamila", "Hoda", "Samira"), 
			Country.ZAMBIA, CList.newList("Chanda", "Mulilo", "Sonkwe", "Chikondi", "Jermaine", "Tiwonge", "Halima", "Thandiwe", "Mukuma", "Jika"), 
			Country.NEW_ZEALAND, CList.newList("Matthew", "Calum", "Hayden", "Corey", "Mitchell", "Charlotte", "Molly", "Olivia", "Phoebe", "Stacey"));
	//@formatter:on

	public static Map<Country, List<String>> getCountryNamesMap() {
		return countryNamesMap;
	}

	// For internationalization (i18n) of country names - see 'messages_en.properties'
	static String language = "en";
	public static ResourceBundle bundle = ResourceBundle.getBundle("messages", Locale.forLanguageTag(language));

	protected static List<Client> clients = new ArrayList<>(); // You may use static members without any restrictions; they won't be saved in database

	// Members

	@SqlColumn(notNull = true)
	public String firstName;
	public Gender gender;

	public Country country;

	public Bike.Size bikeSize;

	public double disposableMoney = 12000.0;

	public Map<String, Double> wantedBikesMaxPriceMap; // Map is saved in separate table

	// Note: If no specific constructor is defined, the necessary default constructor is automatically available (Java behavior).

	// Accumulations (see Bike.java)

	@Accumulation
	public Set<Order> orders;

	// Transient members (marked by 'transient' keyword and will not be registered and stored in database)

	public transient List<Class<? extends Bike>> bikeTypesStillNotOrdered = new ArrayList<>();

	// Methods

	@Override
	public String toString() {
		return (firstName + " from " + CResource.i18n(bundle, country.name()));
	}

	public void init(String firstName, Gender gender, Country country, Bike.Size bikeSize, Double disposableMoney) {

		this.firstName = firstName;
		this.country = country;
		this.gender = gender;
		this.bikeSize = bikeSize;
		this.disposableMoney = disposableMoney;

		wantedBikesMaxPriceMap.put(RaceBike.class.getSimpleName(), disposableMoney);
		bikeTypesStillNotOrdered.add(RaceBike.class);

		wantedBikesMaxPriceMap.put(CityBike.class.getSimpleName(), disposableMoney);
		bikeTypesStillNotOrdered.add(CityBike.class);

		wantedBikesMaxPriceMap.put(MTB.class.getSimpleName(), disposableMoney);
		bikeTypesStillNotOrdered.add(MTB.class);

		clients.add(this);
	}

	// Client thread

	// Order bikes and pay (or cancel) orders
	public class OrderBikes implements Runnable {

		Counters counters = null;
		Client client = null;

		public OrderBikes(
				Client client,
				Counters counters) {

			this.client = client;
			this.counters = counters;
		}

		// Check if bike can be ordered, if yes order it and wait for order processing
		private <T extends Bike> Order orderBike(Class<T> bikeType) throws Exception {

			// Predicate<? super Bike> isBikeAvailableInSize = b -> !(gender == Gender.MALE && b.isForWoman) && b.price.doubleValue() <= wantedBikesMaxPriceMap.get(bikeType.getSimpleName())
			// && b.sizes.contains(bikeSize) && b.availabilityMap.get(bikeSize) != null && b.availabilityMap.get(bikeSize) > 0;

			// Allocate exclusively (pre-select) small as possible amount of bikes matching order condition where one of them then will be ordered
			String whereClause = "DOM_BIKE.PRICE <= " + wantedBikesMaxPriceMap.get(bikeType.getSimpleName());
			if (gender == Gender.MALE) {
				whereClause += " AND DOM_BIKE.IS_FOR_WOMAN = '0'";
			}

			Set<T> bikes = BikeStoreApp.sdc.allocateObjectsExclusively(bikeType, Bike.InProgress.class, whereClause, 0, null);
			Iterator<T> it = bikes.iterator();

			// Check if one of the exclusively allocated bike can be ordered
			Bike orderedBike = null;
			while (it.hasNext()) {
				Bike bike = it.next();
				try {
					synchronized (bike) {
						if (orderedBike == null && bike.sizes.contains(bikeSize) && bike.availabilityMap.get(bikeSize) > 0) {
							bike.availabilityMap.put(bikeSize, bike.availabilityMap.get(bikeSize) - 1); // Decrement availability count
							orderedBike = bike;
							// Do not break; here to ensure releasing checked bike from exclusive use
						}
					}
				}
				catch (Exception ex) {
					log.error("{} - Bike: {}, availability map: {}, bike size: {}", ex.getClass().getSimpleName(), bike, bike.availabilityMap, bikeSize);
				}
				finally {
					BikeStoreApp.sdc.releaseObject(bike, Bike.InProgress.class, null);
				}
			}
			if (orderedBike == null) {
				return null;
			}

			// Save ordered bike with decremented availability count
			orderedBike.save(); // save() method of domain object itself does not throw an exception but logs (SQL) exception occurred
			log.info("'{}' orders bike '{}' ({})", client, orderedBike, bikeSize);

			// Create order using constructor and register and save it explicitly - so ensuring that only a fully initialized order will be registered and can be found by order processing thread
			// Note: Because of 'orderedBike is not 'static' enough create() ad createAndSave() using init function for setting bike of order would not compile
			Order order = new Order(orderedBike, client);
			BikeStoreApp.sdc.register(order);
			BikeStoreApp.sdc.save(order); // save() method of domain controller throws and logs (SQL) exception occurred - using both methods here is only for demonstration
			counters.numberOfOrdersCreated++;

			return order;
		}

		// Pay bike and wait for delivery or cancel order on less money
		private boolean payBikeOrCancelOrder(Order order) throws Exception {

			if (order.bike.price.doubleValue() <= disposableMoney) {

				log.info("Pay price for order '{}' ({}$). (resting disposable money: {}$)", order, order.bike.price, disposableMoney);
				order.payDate = LocalDateTime.now();
				BikeStoreApp.sdc.save(order);
				disposableMoney -= order.bike.price.doubleValue();
				return true;
			}
			else {
				log.info("Cancel order '{}' because client is not able to pay the price of {}$. (disposable money is only: {}$)", order, order.bike.price, disposableMoney);
				order.bike.incrementAvailableCount(bikeSize); // Give bike free again for ordering after client was unable to pay bike
				order.wasCanceled = true;
				order.delete(); // domain object's delete() method used here does not throw SQLException but logs it, delete() method of domain controller throws and logs SQLException occurred
				counters.numberOfOrdersCanceledByInabilityToPay++;
				return false;
			}
		}

		private int LOOP_DELAY = 100;
		private int LOOP_COUNT = 50;

		@Override
		public void run() {

			MDC.put("name", firstName);

			if (log.isDebugEnabled()) {
				log.debug("Client thread for '{}' started", client);
			}

			try {
				// Try to order one bike of any of the three bike types...
				Iterator<Class<? extends Bike>> it = bikeTypesStillNotOrdered.iterator();
				while (it.hasNext()) {

					// Try to find orderable bike of wanted type
					Order order = orderBike(it.next());
					if (order != null) {
						it.remove(); // Bike of this type now was ordered and shall not be ordered again by this client

						// Wait until invoice was sent
						int i;
						for (i = 0; i < LOOP_COUNT && order.invoiceDate == null; i++) {
							BikeStoreApp.sdc.reload(order);
							Thread.sleep(LOOP_DELAY);
						}
						if (order.invoiceDate == null) {
							log.warn("Order processing time exceeded!");
							Order.orderProcessingExceededCount++;
						}
						else {
							int delay = i * LOOP_DELAY;
							synchronized (Order.orderProcessingDurationMap) { // Protocol order processing duration
								Order.orderProcessingDurationMap.put(delay, Order.orderProcessingDurationMap.computeIfAbsent(delay, v -> 0) + 1);
							}
						}

						// Pay bike (or cancel order) and wait until delivery
						boolean bikeWasPayed = payBikeOrCancelOrder(order);
						if (bikeWasPayed) {
							for (i = 0; i < LOOP_COUNT && order.deliveryDate == null; i++) {
								BikeStoreApp.sdc.reload(order);
								Thread.sleep(LOOP_DELAY);
							}
							if (order.deliveryDate == null) {
								log.warn("Bike delivery time exceeded ");
								Order.bikeDeliveryExceededCount++;
							}
							else {
								int delay = i * LOOP_DELAY;
								synchronized (Order.bikeDeliveryDurationMap) { // Protocol delivery duration
									Order.bikeDeliveryDurationMap.put(delay, Order.bikeDeliveryDurationMap.computeIfAbsent(delay, v -> 0) + 1);
								}
							}
						}
					}

					Thread.sleep(BikeStoreApp.ORDER_DELAY_TIME_MS);
				}

				log.info("Client thread for '{}' ended. Still not ordered bike types: {}", client,
						Client.this.bikeTypesStillNotOrdered.stream().map(Class::getSimpleName).collect(Collectors.toList()));
			}
			catch (InterruptedException e) {
				log.warn("Client thread for '{}' ended due to interruption", client);
				Thread.currentThread().interrupt();
			}
			catch (Exception e) { // TODO: Exception handling
				log.error(Common.exceptionStackToString(e));
			}
		}
	}

}
