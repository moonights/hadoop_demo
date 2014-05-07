package com.hadoop.examples.ship;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Random;

import com.hadoop.utils.HdfsFileUtil;
/**
 * 
 * @author moonights
 *
 */
public class ShipDataGenerator {

	static String[] shipNames = {
		"Abendstern	a","Abendstern	a","Abendstern	a","Blaue Weite	a","Casanova		a",
		"Delphin	a","Die Fger a","Entdecker	a","Fegefeuer","Furchtlos",
		"Leise Jerin	a","Mond	a","Morgenwind	a","Narrenwind	a","Navigator	a","Nordstern	a","Rote Theresa	a",
		"Schneller Aal	a","Rasender Falke	a","Renegat	a","Rochenfger	a","Schatzsucher	a",
		"Schwertfisch",
		"Seefalke	a","Seekatze","Seele",
		"Windjagd	a","Wirbelwind","Elise","Alte Lotte",
		"Blauer Stern	a","Brise","Brummb","Butterblume",
		"Wei Julia	a","Weir Schwan	a","Wellenjer	a","Windbote	a",
		"Abendstern	b","Abendstern	b","Abendstern	b","Blaue Weite	b","Casanova		b",
		"Delphin	a","Die Fger a","Entdecker	a","Fegefeuer","Furchtlos",
		"Leise Jerin	b","Mond	b","Morgenwind	b","Narrenwind	b","Navigator	b","Nordstern	a","Rote Theresa	a",
		"Schneller Aal	b","Rasender Falke	a","Renegat	a","Rochenfger	a","Schatzsucher	a",
		"Schwertfisch",
		"Seefalke	b","Seekatze","Seele",
		"Windjagd	b","Wirbelwind	b","Elise","Alte Lotte",
		"Blauer Stern	b","Brise	b","Brummb","Butterblume",
		"Wei Julia	b","Weir Schwan	b","Wellenjer	b","Windbote	b"
	};
	
	public static void main(String[] args) throws Exception {
		
		FileWriter outFile = new FileWriter("/home/hadoop/tmp/shipname2.txt");
		PrintWriter out = new PrintWriter(outFile);
		
		Random randomGenerator = new Random();
		for(int i = 0; i <= 200; i++) {
			int randomShip = randomGenerator.nextInt(shipNames.length);
			String shipName = shipNames[randomShip];
			out.println(shipName);
		}
		out.close();
		outFile.close();
		//创建完成后 上传 文件 到HDFS
//		HdsfFileUtil.makeDir("ship");
		HdfsFileUtil.uploadToHdfs("/home/hadoop/tmp/shipname2.txt", "hdfs://hadoop.master:9000/ship/input/shipname2.txt");
	}
}
