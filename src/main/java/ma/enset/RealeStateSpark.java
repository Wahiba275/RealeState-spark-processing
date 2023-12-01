package ma.enset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class RealeStateSpark {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("test mysql").master("local[*]").getOrCreate();
        Map<String , String > options = new HashMap< >( ) ;
        options.put( "driver" ,  "com.mysql.cj.jdbc.Driver" );
        options.put( "url" , "jdbc:mysql://localhost:3306/db_imomaroc" ) ;
        options.put( "user" , "root") ;
        options.put( "password" , "" ) ;

        Dataset<Row>  dataset = ss.read().format("jdbc")
                .options(options)
                .option("query" , "select * from PROJETS ")
                .load();
        dataset.show();
        Dataset<Row>  dataset1 = ss.read().format("jdbc")
                .options(options)
                .option("query" , "select * from PROJETS WHERE DATE_FIN >= CURRENT_DATE ")
                .load();
        dataset1.show();
        Dataset<Row>  dataset2= ss.read().format("jdbc")
                .options(options)
                .option("query" , "SELECT T.ID_PROJET, P.TITRE,  COUNT(T.ID_TACHE) AS NOMBRE\n" +
                        "FROM TACHES T\n" +
                        "JOIN PROJETS P ON T.ID_PROJET = P.ID_PROJET\n" +
                        "WHERE DATEDIFF(P.DATE_FIN, P.DATE_DEBUT) > 30\n" +
                        "GROUP BY P.ID_PROJET\n" )
                .load();
        dataset2.show();
        Dataset<Row>  dataset3= ss.read().format("jdbc")
                .options(options)
                .option("query" , "SELECT  P.TITRE as  TITRE_PROJET ,T.TITRE as TITRE_TACHE , DATEDIFF(CURRENT_DATE, T.DATE_FIN) AS DUREE_RETARD \n" +
                        "FROM TACHES T\n" +
                        "JOIN PROJETS P ON T.ID_PROJET = P.ID_PROJET\n" +
                        "WHERE T.TERMINE=0\n" )
                .load();
        dataset3.show();
    }
}