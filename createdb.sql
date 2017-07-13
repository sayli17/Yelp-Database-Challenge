CREATE TABLE Yelp_User(
user_id VARCHAR2(500) NOT NULL,
user_name VARCHAR2(500) NOT NULL,
member_since VARCHAR2(7) NOT NULL,
review_count NUMBER NOT NULL,
average_stars NUMBER(3,2) NOT NULL,
votes_funny NUMBER NOT NULL,
votes_useful NUMBER NOT NULL,
votes_cool NUMBER NOT NULL,
PRIMARY KEY (user_id)
);

--SELECT * FROM Yelp_User;
--SELECT COUNT(*) FROM Yelp_User;

CREATE TABLE User_friend(
user_id VARCHAR2(500) NOT NULL,
friend_id VARCHAR2(500) NOT NULL,
CONSTRAINT user_friend_pk PRIMARY KEY (user_id, friend_id),
CONSTRAINT user_id_fk FOREIGN KEY(user_id) REFERENCES Yelp_User(user_id) ON DELETE CASCADE
--CONSTRAINT friend_id_fk FOREIGN KEY(friend_id) REFERENCES Yelp_User(user_id) ON DELETE CASCADE
);

--SELECT * FROM User_friend;
--SELECT COUNT(*) FROM User_friend;

CREATE TABLE Business(
business_id VARCHAR2(30),
business_name VARCHAR2(100) NOT NULL,
city VARCHAR2(100) NOT NULL,
state VARCHAR2(100) NOT NULL,
stars NUMBER NOT NULL,
CONSTRAINT business_pk PRIMARY KEY (business_id)
);

--SELECT * FROM Business;
--SELECT COUNT(*) FROM Business;

CREATE TABLE Main_category(
bid VARCHAR2(30) NOT NULL,
main_cat VARCHAR2(100) NOT NULL,
CONSTRAINT main_cat_fk FOREIGN KEY(bid) REFERENCES Business(business_id) ON DELETE CASCADE
);

--SELECT * FROM Main_category;
--SELECT COUNT(*) FROM Main_category;
--SET DEFINE OFF;
--SELECT COUNT(*) FROM Main_category WHERE main_cat='Arts & Entertainment';

CREATE TABLE Sub_category(
bid VARCHAR2(30) NOT NULL,
sub_cat VARCHAR2(100) NOT NULL,
CONSTRAINT sub_cat_fk FOREIGN KEY(bid) REFERENCES Business(business_id) ON DELETE CASCADE
);

--SELECT DISTINCT S.sub_cat FROM Sub_category S, Main_category M
--WHERE M.bid=S.bid AND main_cat='Restaurants'
--ORDER BY S.sub_cat;

--SELECT DISTINCT sub_cat FROM Sub_category;
--SELECT COUNT(*) FROM Sub_category;

CREATE TABLE Checkin(
business_id VARCHAR2(30) NOT NULL,
day int NOT NULL,
hour int NOT NULL,
checkin_count int NOT NULL,
CONSTRAINT business_id_fk FOREIGN KEY(business_id) REFERENCES Business(business_id) ON DELETE CASCADE
);

--SELECT * FROM Checkin;
--SELECT COUNT(*) FROM Checkin;

CREATE TABLE Review(
review_id VARCHAR2(30) NOT NULL,
user_id VARCHAR2(30) NOT NULL,
business_id VARCHAR2(30) NOT NULL,
review_date DATE NOT NULL,
stars NUMBER NOT NULL,
votes_useful NUMBER NOT NULL,
votes_funny NUMBER NOT NULL,
votes_cool NUMBER NOT NULL,
text CLOB NOT NULL,
CONSTRAINT review_pk PRIMARY KEY (review_id),
CONSTRAINT u_id_fk FOREIGN KEY(user_id) REFERENCES Yelp_User(user_id) ON DELETE CASCADE,
CONSTRAINT bus_id_fk FOREIGN KEY(business_id) REFERENCES Business(business_id) ON DELETE CASCADE,
CONSTRAINT no_of_stars CHECK (stars BETWEEN 1 AND 5)
);

--SELECT * FROM Review;
--SELECT COUNT(*) FROM Review;

ALTER TABLE Review ADD total_votes NUMBER;
UPDATE Review SET total_votes = votes_useful + votes_funny + votes_cool;