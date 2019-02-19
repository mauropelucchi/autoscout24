package autoscout24;

import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.jsoup.nodes.Element;

/**
 * AutoScout 24 Scraper
 *
 */
public class App {
	public static final int MAX_PAGES = 20;

	public static void main(String[] args) {
		MongoClient mongoClient = null;
		try {
			mongoClient = MongoClients.create("mongodb://bigdata03:27017");
			MongoDatabase db = mongoClient.getDatabase("autoscout24");
			MongoCollection<org.bson.Document> c = db.getCollection("auto_usate");

			Arrays.asList(Location.LOCATIONS).parallelStream().forEach(location -> {
				try {
					System.out.println(" --------- " + location + " --------------- ");
					location = location.replaceAll(" ", "-").toLowerCase();
					final String my_loc = URLEncoder.encode(location, "UTF-8");
					Arrays.asList(Brand.BRANDS).parallelStream().forEach(brand -> {
						System.out.println(" --------- " + brand + " --------------- ");
						brand = brand.replaceAll(" ", "-").toLowerCase();

						for (int page = 1; page <= MAX_PAGES; page++) {
							try {
								Document d = Jsoup.connect("https://www.autoscout24.it/lst/" + brand + "/" + my_loc
										+ "?sort=standard&size=20&desc=0&offer=J%2CU%2CO%2CD%2CS&ustate=N%2CU&cy=I&atype=C&page="
										+ page).userAgent("Mozilla").timeout(3000).get();
								d.setBaseUri("https://www.autoscout24.it/lst/" + brand + "/" + my_loc
										+ "?sort=standard&desc=0&offer=J%2CU%2CO%2CD%2CS&ustate=N%2CU&cy=I&atype=C&page="
										+ page);
								Elements e = d.select("[data-item-name=\"listing-summary-container\"]");
								for (Element element : e) {
									String url = element.select("[data-item-name=\"detail-page-link\"]").first()
											.absUrl("href");
									System.out.println(url);
									// create auto
									org.bson.Document query = new org.bson.Document();
									query.append("_id", url);
									if (c.countDocuments(query) <= 0) {

										String titolo = element.select("[data-item-name=\"headline\"]").text();
										String descrizione = element.select("[data-item-name=\"sub-headline\"]").text();
										String prezzo = element.select("[data-item-name=\"price\"]").text();
										String prezzoLabel = element.select("[data-item-name=\"price-label\"] > div")
												.attr("class");
										Elements dettagli = element.select("[data-item-name=\"vehicle-details\"] > li");
										String km = dettagli.get(0).text();
										String imm = dettagli.get(1).text();
										String cavalli = dettagli.get(2).text();
										String usato = dettagli.get(3).text();
										String nprop = dettagli.get(4).text();
										String cambio = dettagli.get(5).text();
										String alimentazione = dettagli.get(6).text();
										String consumo = dettagli.get(7).text();
										String emissioni = dettagli.get(8).text();
										String tipoVenditore = "concessionario";
										if (element.select("[data-test=\"seller-type\"]").size() > 0) {
											tipoVenditore = "privato";
										}
										String indirizzo = element.select("[data-test=\"address\"]").text();
										String concessionario = "";
										if (element.select("[data-test=\"company-name\"]").size() > 0) {
											concessionario = element.select("[data-test=\"company-name\"]").text();
										}

										try {
											Document auto = Jsoup.connect(url).userAgent("Mozilla").timeout(3000).get();
											String tipoAuto = (auto.select(".cldt-headline h4").size() > 0)
													? auto.select(".cldt-headline h4").first().text()
													: "";
											Elements dettagliDL_Lista = auto.select(".sc-grid-row dl");
											Map<String, String> mappa = new HashMap<String, String>();

											for (Element dettagliDL : dettagliDL_Lista) {
												Elements dettagli2 = dettagliDL.children();
												Iterator<Element> i = dettagli2.iterator();
												Element dettaglio = i.next();
												while (i.hasNext()) {
													String key = dettaglio.text().replaceAll(" ", "_").replaceAll("\\.",
															"");
													dettaglio = i.next();
													String value = dettaglio.text();
													mappa.put(key, value);
													// System.out.println(key + " > " + value);
													if (i.hasNext()) {
														dettaglio = i.next();
													}
												}
											}
											String equip = auto.select("[data-item-name=\"equipments\"]").text();

											// create auto

											org.bson.Document autoDoc = new org.bson.Document();
											autoDoc.append("_id", url);
											autoDoc.append("title", titolo);
											autoDoc.append("descrizione", descrizione);
											autoDoc.append("url", url);
											autoDoc.append("prezzo", prezzo);
											autoDoc.append("prezzoLabel", prezzoLabel);
											autoDoc.append("equip", equip);
											for (String key : mappa.keySet()) {
												autoDoc.append(key, mappa.get(key));
											}
											autoDoc.append("tipoAuto", tipoAuto);
											autoDoc.append("concessionario", concessionario);
											autoDoc.append("indirizzo", indirizzo);
											autoDoc.append("km", km);
											autoDoc.append("imm", imm);
											autoDoc.append("cavalli", cavalli);
											autoDoc.append("usato", usato);
											autoDoc.append("nprop", nprop);
											autoDoc.append("cambio", cambio);
											autoDoc.append("alimentazione", alimentazione);
											autoDoc.append("consumo", consumo);
											autoDoc.append("emissioni", emissioni);
											autoDoc.append("tipoVenditore", tipoVenditore);
											c.insertOne(autoDoc);

										} catch (Exception e1) {
											System.out.println("Error for url " + url);
											e1.printStackTrace();
										}

									}

									try {
										Thread.sleep(5);
									} catch (Exception e1) {
										e1.printStackTrace();
									}
								}
							} catch (Exception e) {
								e.printStackTrace();
							}

						}
					});

				} catch (Exception ex) {
					ex.printStackTrace();
				}
			});

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (mongoClient != null) {
				try {
					mongoClient.close();
				} catch (Exception e) {
				}
			}
		}
	}
}
