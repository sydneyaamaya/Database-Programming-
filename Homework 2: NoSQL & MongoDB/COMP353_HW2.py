import pymongo 
import datetime, pprint
from pymongo import MongoClient

client = MongoClient("mongodb+srv://lucuser:csclassluc@luccluster.jvo5zef.mongodb.net/?appName=LUCcluster")

db = client.sample_airbnb

"""
Query #1:

Find the top 3 most expensive listings in terms of monthly price in Australia 
with exactly 2 bedrooms. Only return the top 3 results, that is, limit to 3 
and sort in descending order.
(Fields): _id, name, price, property type

"""

print("Query #1 Results:")

q1_filter = {
    "address.country": "Australia",
    "bedrooms": 2,
    "monthly_price": {"$exists": True}
}

q1_projection = {
    "_id": 1,
    "name": 1,
    "monthly_price": 1,
    "property_type": 1
}

q1_cursor = (
    db.listingsAndReviews.find(q1_filter, q1_projection)
        .sort("monthly_price", -1)
        .limit(3)
)

for doc in q1_cursor:
    pprint.pprint(doc)

""" 
Query #2: 

Find the listings with country code ‘US’, room type ‘Entire home/apt’, 
the minimum number of nights required is 3, and the price range is 
between $700 to $1,000. The price ranges should be inclusive. 
Sort the results by price in ascending order.
(Fields): _id, name, price, bedrooms, number of reviews

"""

print("\nQuery #2 Results:")

def query_2():
        pipeline = [
        {
            "$match": {
                "address.country_code": "US",
                "room_type": "Entire home/apt",
            }
        },
        {
            "$addFields": {
                "minimum_nights": {
                    "$toInt": "$minimum_nights"
                },
                "price_num":{
                    "$toDouble": "$price"
                }
            }
        },
        {
            "$match": {
                "minimum_nights": 3,
                "price_num": {"$gte": 700, "$lte": 1000},
            }
        },
        {
            "$sort": {"price_num": 1}
        },
        {
            "$project": {
            "_id": 1,
            "name": 1,
            "price": 1,
            "bedrooms": 1,
            "number_of_reviews": 1,
            }
        }
    ]
        results = db.listingsAndReviews.aggregate(pipeline)
        
        for result in results:
            pprint.pprint(result)

query_2()

"""
Query #3:

Find the top 5 most expensive listings in terms of monthly price, but where 
the number of beds in the listing is greater than the number of bedrooms or the 
number of beds in the listing is greater than the number of people this listing 
can accommodate (accommodates). Your output should be limited to 5 results sorted 
in descending order by monthly price. Hint: Consider using the $expr operator.
(Fields): _id, name, beds, bedrooms, accommodates

"""
print("\nQuery #3 results:")   

q3_filter = {
    "monthly_price": {"$exists": True},
    "$expr": {
        "$or": [
            {"$gt": ["$beds", "$bedrooms"]},
            {"$gt": ["$beds", "$accommodates"]}
        ]
    }
}

q3_projection = {
    "_id": 1,
    "name": 1,
    "beds": 1,
    "bedrooms": 1,
    "accommodates": 1,
    "monthly_price": 1   # (kept for sorting / clarity)
}

q3_cursor = (
    db.listingsAndReviews.find(q3_filter, q3_projection)
        .sort("monthly_price", -1)
        .limit(5)
)

for doc in q3_cursor:
    pprint.pprint(doc)

"""
Query #4:

Find the listings with at least these amenities: ‘Wifi’, ‘Kitchen’, ‘Pets allowed’, 
and a minimum 6 bedrooms. In the output, in addition to outputting ‘name’ and 
‘price’, also include the total number of amenities contained in the listing. 
Sort the output by price in ascending order. Hint: There is an operator 
specifically for getting the size of an array.
(Fields): _id, name, price, amenity count

"""

print("\nQuery #4 results:")

def query_4():
        pipeline = [
        {
              "$match":{
               "bedrooms": {"$gte": 6}, 
               "amenities": {"$all": ["Wifi", "Kitchen", "Pets allowed"]}
            } 
        },
        {
            "$addFields":{
                "price_num": {"$toDouble": "$price"}
            }  
        },
        {
              "$sort": {"price_num": 1}
        },
        {
            "$project":{
                    "_id": 1,
                    "name": 1, 
                    "price": 1,
                    "amenity_count": {"$size": "$amenities"}
            }
        }
    ]
        results = db.listingsAndReviews.aggregate(pipeline)
        
        for result in results:
            pprint.pprint(result)

query_4()

"""
Query #5:

For listings with 10 or more reviews, a review score of 80 or greater, 
and a host response rate of 90 or greater, compute the average review 
score rating by government area. Round the average review score rating 
to 2 decimal points. Sort the output by the government area name in 
ascending order and limit the output to 5 government areas. Hint: 
Consider using an aggregation pipeline with $match, $group, $project, 
$sort, and $limit.
(Fields): avg rating, government area

"""
print("\nQuery #5 results:")

q5_pipeline = [
    {
        "$match": {
            "number_of_reviews": { "$gte": 10 },
            "review_scores.review_scores_rating": { "$gte": 80 },
            "host.host_response_rate": { "$gte": 90 }
        }
    },
    {
        "$group": {
            "_id": "$address.government_area",
            "avg_rating": {
                "$avg": "$review_scores.review_scores_rating"
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "government_area": "$_id",
            "avg_rating": { "$round": ["$avg_rating", 2] }
        }
    },
    {
        "$sort": { "government_area": 1 }
    },
    {
        "$limit": 5
    }
]

q5_results = db.listingsAndReviews.aggregate(q5_pipeline)
for doc in q5_results:
    pprint.pprint(doc)

"""
Query #6:

For listings that accommodate 15 or more guests, compute the average price, 
the average cleaning fee, and the count of the listings per property type. 
If the cleaning free is null or missing, replace the null or missing with a 
default value of 0.0. This data cleanup step must be done in the query itself. 
Round the average price and average cleaning fee to 2 decimal points. Sort the 
final list of property types by average price in descending order and limit the
output to 5 property types. Hint: Consider using an aggregation pipeline with 
$match, $group, $project, $sort, and $limit. Consider using the $ifNull operator.
(Fields): property type, avg price, avg cleaning fee, listing count

"""

print("\nQuery #6 results:")

def query_6():
      pipeline = [
            {
                "$match":{
                        "accommodates": {"$gte": 15}
                }
            },
            {
                 "$addFields":{
                        "price_num": {"$toDouble": "$price"},
                        "cleaning_fee_num": {
                             "$toDouble": {
                                    "$ifNull": ["$cleaning_fee", 0.0]
                             }
                        }
                 } 
            },
            {
                    "$group":{
                            "_id": "$property_type",
                            "avg_price": {"$avg": "$price_num"},
                            "avg_cleaning_fee": {"$avg": "$cleaning_fee_num"},
                            "listing_count": {"$sum": 1}
                            
                    }
            },
            {
                    "$sort":{"avg_price": -1}
            },
            {
                  "$limit": 5
            },
            {
                 "$project":{
                      "property_type": "$_id",
                      "avg_price": {"$round": ["$avg_price", 2]},
                      "avg_cleaning_fee": {"$round": ["$avg_cleaning_fee", 2]},
                      "listing_count": 1
                 } 
            }
      ]
      results = db.listingsAndReviews.aggregate(pipeline)
      for result in results:
            pprint.pprint(result)

query_6()

