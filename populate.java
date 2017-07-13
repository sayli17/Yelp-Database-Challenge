/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelpjson;

import java.io.*;
import static java.lang.Math.toIntExact;
import java.util.HashMap.*;
import java.util.Iterator;
import java.util.Set;
import java.util.HashMap;
import java.util.*;
import java.sql.*;
import java.time.LocalDate;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 *
 * @author Sayli
 */
public class populate {

    /**
     * @param args the command line arguments
     */
    private static final String host = "localhost";
    private static String dbName = "JSON3";
    private static int port = 1521;
    private static String username = "SYSTEM";
    private static String password = "tiger";

    public static void main(String[] args) throws SQLException, FileNotFoundException, IOException, JSONException {
        // TODO code application logic here

//        String file_business = "C:\\Users\\Sayli\\Desktop\\DBA\\assignments\\Assignment 3 JSON\\YelpDataset\\yelp_business.json";
//        String file_user = "C:\\Users\\Sayli\\Desktop\\DBA\\assignments\\Assignment 3 JSON\\YelpDataset\\yelp_user.json";
//        String file_review = "C:\\Users\\Sayli\\Desktop\\DBA\\assignments\\Assignment 3 JSON\\YelpDataset\\yelp_review.json";
//        String file_checkin = "C:\\Users\\Sayli\\Desktop\\DBA\\assignments\\Assignment 3 JSON\\YelpDataset\\yelp_checkin.json";
        String file_business = "";
        String file_user = "";
        String file_review = "";
        String file_checkin = "";

        if (args.length >= 4) {
            file_business = args[0];
            file_review = args[1];
            file_checkin = args[2];
            file_user = args[3];
        }

        //Loading the driver
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException cnfe) {
            System.out.println("Error loading driver: " + cnfe);
        }

        //Define the connection
        String oracleURL = "jdbc:oracle:thin:@" + host + ":" + port + ":" + dbName;

        //Establishing the connection
        Connection connection = DriverManager.getConnection(oracleURL, username, password);

        insert_yelp_user(connection, file_user);
        insert_user_friend(connection, file_user);
        insert_business(connection, file_business);
        insert_category(connection, file_business);
        insert_checkin(connection, file_checkin);
        insert_review(connection, file_review);

        //Closing the connection
        connection.close();
    }

    public static void insert_yelp_user(Connection connection, String filename) throws FileNotFoundException, IOException {

        FileInputStream f = null;
        BufferedReader br = null;

        try {
            f = new FileInputStream(filename);
            br = new BufferedReader(new InputStreamReader(f));
            String line = br.readLine();

            while (line != null) {

                JSONObject jsonObject = (JSONObject) new JSONTokener(line).nextValue();
                String user_id = (String) jsonObject.getString("user_id");
                String user_name = (String) jsonObject.getString("name");
                String member_since = (String) jsonObject.getString("yelping_since");
                int review_count = (Integer) jsonObject.getInt("review_count");
                double average_stars = (Double) jsonObject.getDouble("average_stars");

                JSONObject json_votes = (JSONObject) jsonObject.getJSONObject("votes");
                int votes_funny = (Integer) json_votes.getInt("funny");
                int votes_useful = (Integer) json_votes.getInt("useful");
                int votes_cool = (Integer) json_votes.getInt("cool");

                PreparedStatement stmt = connection.prepareStatement("INSERT INTO Yelp_User"
                        + "(user_id, user_name, member_since, review_count, average_stars,"
                        + " votes_funny, votes_useful, votes_cool) VALUES"
                        + "(?, ?, ?, ?, ?, ?, ?, ?)");
                stmt.setString(1, user_id);
                stmt.setString(2, user_name);
                stmt.setString(3, member_since);
                stmt.setInt(4, review_count);
                stmt.setDouble(5, average_stars);
                stmt.setInt(6, votes_funny);
                stmt.setInt(7, votes_useful);
                stmt.setInt(8, votes_cool);

                // execute insert SQL stetement
                stmt.executeUpdate();

                stmt.close();

                line = br.readLine();
            }

            f.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void insert_user_friend(Connection connection, String filename) throws FileNotFoundException, JSONException {
        FileInputStream f = null;
        BufferedReader br = null;

        try {
            f = new FileInputStream(filename);
            br = new BufferedReader(new InputStreamReader(f));
            String line = br.readLine();

            while (line != null) {

                JSONObject jsonObject = (JSONObject) new JSONTokener(line).nextValue();
                String user_id = (String) jsonObject.getString("user_id");
                System.out.println("User id:" + user_id);

                JSONArray friendList = (JSONArray) jsonObject.get("friends");
                int len = friendList.length();
                String stringArray[] = new String[len];
                String friend_id = null;

                if (len != 0) {
                    for (int i = 0; i < len; i++) {
                        stringArray[i] = friendList.get(i).toString();
                        System.out.println("Friend id:" + stringArray[i]);
                    }

                    for (int i = 0; i < friendList.length(); i++) {

                        friend_id = stringArray[i];
                        PreparedStatement stmt = connection.prepareStatement("INSERT INTO User_friend"
                                + "(user_id, friend_id) VALUES(?, ?)");
                        stmt.setString(1, user_id);
                        stmt.setString(2, friend_id);

                        // execute insert SQL stetement
                        stmt.executeUpdate();
                        System.out.println("Success!");
                        stmt.close();
                    }
                }
                line = br.readLine();
            }
            f.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void insert_business(Connection connection, String filename) throws FileNotFoundException {
        FileInputStream f = null;
        BufferedReader br = null;

        try {
            f = new FileInputStream(filename);
            br = new BufferedReader(new InputStreamReader(f));
            String line = br.readLine();

            while (line != null) {

                JSONObject jsonObject = (JSONObject) new JSONTokener(line).nextValue();
                String business_id = (String) jsonObject.getString("business_id");
                String business_name = (String) jsonObject.getString("name");
                String city = (String) jsonObject.getString("city");
                String state = (String) jsonObject.getString("state");
                Double stars = (Double) jsonObject.getDouble("stars");

                PreparedStatement stmt = connection.prepareStatement("INSERT INTO Business"
                        + "(business_id, business_name, city, state, stars) VALUES"
                        + "(?, ?, ?, ?, ?)");
                stmt.setString(1, business_id);
                stmt.setString(2, business_name);
                stmt.setString(3, city);
                stmt.setString(4, state);
                stmt.setDouble(5, stars);

                // execute insert SQL stetement
                stmt.executeUpdate();

                stmt.close();

                line = br.readLine();
            }

            f.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void insert_category(Connection connection, String filename) throws FileNotFoundException {
        FileInputStream f = null;
        BufferedReader br = null;

        try {
            f = new FileInputStream(filename);
            br = new BufferedReader(new InputStreamReader(f));
            String line = br.readLine();
            String main_cat = null;
            String sub_cat = null;

            while (line != null) {

                JSONObject jsonObject = (JSONObject) new JSONTokener(line).nextValue();
                String bid = (String) jsonObject.getString("business_id");

                JSONArray arrObj = (JSONArray) jsonObject.get("categories");
                int len = arrObj.length();
                String stringArray[] = new String[len];

                for (int i = 0; i < len; i++) {
                    stringArray[i] = arrObj.get(i).toString();
//                    System.out.print(stringArray[i]);
                }
//                System.out.println();

                String main_category[] = {"Active Life", "Arts & Entertainment", "Automotive", "Car Rental",
                    "Cafes", "Beauty & Spas", "Convenience Stores", "Dentists", "Doctors", "Drugstores",
                    "Department Stores", "Education", "Event Planning & Services", "Flowers & Gifts",
                    "Food", "Health & Medical", "Home Services", "Home & Garden", "Hospitals",
                    "Hotels & Travel", "Hardware Stores", "Grocery", "Medical Centers", "Nurseries & Gardening",
                    "Nightlife", "Restaurants", "Shopping", "Transportation"};

                for (int i = 0; i < len; i++) {
                    if (Arrays.asList(main_category).contains(stringArray[i])) {
                        main_cat = stringArray[i];
                        PreparedStatement stmt = connection.prepareStatement(
                                "INSERT INTO Main_category"
                                + "(bid, main_cat) VALUES"
                                + "(?, ?)");

                        stmt.setString(1, bid);
                        stmt.setString(2, main_cat);

                        // execute insert SQL stetement
                        stmt.executeUpdate();

                        stmt.close();
                    } else {
                        sub_cat = stringArray[i];
                        PreparedStatement stmt = connection.prepareStatement(
                                "INSERT INTO Sub_category"
                                + "(bid, sub_cat) VALUES"
                                + "(?, ?)");

                        stmt.setString(1, bid);
                        stmt.setString(2, sub_cat);

                        // execute insert SQL stetement
                        stmt.executeUpdate();

                        stmt.close();
                    }
                }
                line = br.readLine();
            }
            f.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void insert_checkin(Connection connection, String filename) throws FileNotFoundException, JSONException, IOException, SQLException {
        FileInputStream f = null;
        BufferedReader br = null;

        f = new FileInputStream(filename);
        br = new BufferedReader(new InputStreamReader(f));
        String line = br.readLine();

        while (line != null) {

            JSONObject jsonObject = (JSONObject) new JSONTokener(line).nextValue();
            String business_id = (String) jsonObject.getString("business_id");

            JSONObject checkin = jsonObject.getJSONObject("checkin_info");

            String checkin_info = checkin.toString();

            String checkin_items[] = new String[checkin_info.length()];
            checkin_items = checkin_info.trim().split(",");
//            for(int i=0; i< checkin_items.length; i++)
//            System.out.println(checkin_items[i]);

            int day = 0, hour = 0, checkin_count = 0;

            for (int i = 0; i < checkin_items.length; i++) {

                String temp1[] = new String[5];
                temp1 = checkin_items[i].trim().split("-");
//                System.out.println(temp1[0]);
                int index = temp1[0].indexOf("\"") + 1;
//                System.out.println(index);
                temp1[0] = temp1[0].substring(index);
//                System.out.println(temp1[0]);
                hour = Integer.parseInt(temp1[0]);

                String temp2[] = new String[5];
                temp2 = temp1[1].trim().split(":");
                int index1 = temp2[0].indexOf("\"");
                temp2[0] = temp2[0].substring(0, index1);
                day = Integer.parseInt(temp2[0]);

                if (temp2[1].contains("}")) {
                    int index2 = temp2[1].indexOf("}");
                    temp2[1] = temp2[1].substring(0, index2);
                    checkin_count = Integer.parseInt(temp2[1]);
//                    System.out.println("count=" + checkin_count);

                } else {
                    checkin_count = Integer.parseInt(temp2[1]);
//                    System.out.println("count=" + checkin_count);
                }

                PreparedStatement stmt = connection.prepareStatement("INSERT INTO Checkin(business_id, day, hour, "
                        + "checkin_count) VALUES(?,?,?,?)");

                stmt.setString(1, business_id);
                stmt.setInt(2, day);
                stmt.setInt(3, hour);
                stmt.setInt(4, checkin_count);
                stmt.executeUpdate();
                //System.out.println("Success!");
                stmt.close();
            }

            line = br.readLine();
        }

        f.close();

    }

    public static void insert_review(Connection connection, String filename) throws FileNotFoundException {
        FileInputStream f = null;
        BufferedReader br = null;

        try {
            f = new FileInputStream(filename);
            br = new BufferedReader(new InputStreamReader(f));
            String line = br.readLine();

            while (line != null) {

                JSONObject jsonObject = (JSONObject) new JSONTokener(line).nextValue();
                String review_id = (String) jsonObject.getString("review_id");
                String user_id = (String) jsonObject.getString("user_id");
                String business_id = (String) jsonObject.getString("business_id");
                java.sql.Date review_date;
                review_date = java.sql.Date.valueOf((String) jsonObject.get("date"));
                Double stars = (Double) jsonObject.getDouble("stars");

                JSONObject json_votes = (JSONObject) jsonObject.getJSONObject("votes");
                int votes_funny = (Integer) json_votes.getInt("funny");
                int votes_useful = (Integer) json_votes.getInt("useful");
                int votes_cool = (Integer) json_votes.getInt("cool");

                String text = (String) jsonObject.getString("text");

                PreparedStatement stmt = connection.prepareStatement("INSERT INTO Review"
                        + "(review_id, user_id, business_id, review_date, stars, votes_useful,"
                        + "votes_funny, votes_cool, text) VALUES"
                        + "(?, ?, ?, ?, ?, ?, ?, ?, ?)");
                stmt.setString(1, review_id);
                stmt.setString(2, user_id);
                stmt.setString(3, business_id);
                stmt.setDate(4, review_date);
                stmt.setDouble(5, stars);
                stmt.setInt(6, votes_useful);
                stmt.setInt(7, votes_funny);
                stmt.setInt(8, votes_cool);
                stmt.setString(9, text);

                // execute insert SQL stetement
                stmt.executeUpdate();

                stmt.close();

                line = br.readLine();
            }

            f.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
