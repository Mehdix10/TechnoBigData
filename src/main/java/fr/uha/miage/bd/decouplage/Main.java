package fr.uha.miage.bd.decouplage;

import static fr.uha.miage.bd.decouplage.utils.Utils.*;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import fr.uha.miage.bd.decouplage.utils.CloseableIterator;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Main {

	private static final int IDX_CONTINENT = 3;
	private static final int IDX_COUNTRY = 0;
	private static final int IDX_GDP = 5;
	private static final int IDX_GHG = 76;
	private static final int IDX_YEAR = 1;

	public static JavaRDD<Tuple4<String, String, Integer, String>> decouplage(JavaSparkContext sc, File infile,
			String annee) {
		JavaRDD<Tuple5<String /* continent */, String /* country */, String /* gdp */, String /* ghg */, String>> raw = sc
				.textFile(infile.getAbsolutePath(), 14 * 5).filter(l -> !l.startsWith("country,"))
				.map(l -> fromCSV(l, ','))
				.map(l -> new Tuple5<>(l[IDX_CONTINENT], l[IDX_COUNTRY], l[IDX_GDP], l[IDX_GHG], l[IDX_YEAR]))
				.filter(c -> !c._1().isEmpty()).filter(c -> !c._2().isEmpty()).filter(c -> !c._3().isEmpty())
				.filter(c -> !c._4().isEmpty()).filter(c -> !c._5().isEmpty()).filter(c -> c._5().equals(annee.trim()));

		JavaRDD<Tuple5<String /* continent */, String /* country */, Integer /* gdp */, Integer /* ghg */, String>> withNumbers = raw
				.map(c -> {
					int gdp = -1;
					String gdpAsStr = c._3();
					int coma = gdpAsStr.indexOf('.') - 6;
					if (coma > 0) {
						gdpAsStr = gdpAsStr.substring(0, coma);
						try {
							gdp = Integer.parseInt(gdpAsStr);
						} catch (NumberFormatException x) {
							System.err.println("Can't parse GDP for " + c);
						}
					}
					return new Tuple5<>(c._1(), c._2(), gdp, c._4(), c._5());
				}).filter(c -> c._3() > 0).map(c -> {
					int ghg = -1;
					String ghgAsStr = c._4();
					int coma = ghgAsStr.indexOf('.');
					if (coma > 0) {
						ghgAsStr = ghgAsStr.substring(0, coma) + ghgAsStr.substring(coma + 1, coma + 3);
						try {
							ghg = Integer.parseInt(ghgAsStr);
						} catch (NumberFormatException x) {
							System.err.println("Can't parse GHG for " + c);
						}
					}
					return new Tuple5<>(c._1(), c._2(), c._3(), ghg, c._5());
				}).filter(c -> c._4() > 0);

		JavaRDD<Tuple4<String /* continent */, String /* country */, Integer /* gdp / ghg */, String>> coupling = withNumbers
				.map(c -> new Tuple4<>(c._1(), c._2(), Math.multiplyExact(c._3(), 100) / c._4(), c._5())) // !!!!!!!!!!
		;

		return coupling;
	}

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("decouplage");

		String master = System.getProperty("spark.master");
		if (master == null || master.trim().length() == 0) {
			System.out.println("No master found ; running locally");
			conf.setMaster("local[*]").set("spark.driver.host", "127.0.0.1");
		} else {
			System.out.println("Master found to be " + master);
		}

		// Tries to determine necessary jar
		String source = Main.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		if (source.endsWith(".jar")) {
			conf.setJars(new String[] { source });
		}

		File infile = new File("data-18.csv");
		downloadIfAbsent(new URI("https://svn.ensisa.uha.fr/bd/co2/owid-co2-data-continents.csv").toURL(), infile);
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			JavaRDD<Tuple4<String, String, Integer, String>> coupling2017 = decouplage(sc, infile, "2017");
			JavaRDD<Tuple4<String, String, Integer, String>> coupling2022 = decouplage(sc, infile, "2022");
			// ! TODO : Il faut utiliser le pays comme clé
			mapedCoupling2022= coupling2022.
					mapToPair(c -> new Tuple2<>(c._2(), new Tuple2<>(c._1(),  c._3(), c._4())));
			 JavaPairRDD<Tuple2<String, String>, Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> couplingKeyed = coupling2017
					.mapToPair(c -> new Tuple2<>(new Tuple2<>(c._1(), c._4()), new Tuple2<>(c._2(), c._3())))
					.join(mapedCoupling2022)
					.reduceByKey((c1, c2) -> c1._2() >= c2._2() ? c1 : c2);
			List<Tuple2<Tuple2<String /* continent */, String /* Année */>, Tuple2<String /* country */, Integer >>> result = couplingKeyed.collect();
			result.forEach(System.out::println);

		}

		long s = (System.currentTimeMillis() - start) / 1000;
		String dur = String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60));
		System.out.println("Analysis completed in " + dur);
	}
}