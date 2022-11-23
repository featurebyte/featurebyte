FeatureByte’s Feature Discovery is designed to further support data scientists in their feature engineering work.

While:

* the Data Semantics annotation informs them on the nature of data
* the Declarative Framework unleashes their creativity
* and the Feature Store helps them reuse features and quickly push their new features into production,

Feature Discovery helps them uncover features with signals that may not have been captured yet.

### Core Principles

The development of the Feature Discovery is governed by the following principles:

* only meaningful features should be suggested. Data Scientists should not be overwhelmed by a large number of noisy features
* suggestions should well cover feature engineering best practices
* suggestions should not miss important signals

To achieve this, Feature Discovery is built as a rule based tool that:

* relies on the data semantics. If no data semantics is annotated to the data, no suggestion is raised
* codifies the best practices for those semantics
* automatically joins data
* searches features for entities related to the main entity
* is easily extensible by the data scientist for internal usage or by FeatureByte’s community.


### Discovery Outputs

Feature Discovery can be triggered from:

* a use case
* a view and an entity
* or a view column and an entity

Feature Discovery results are in the form of feature recipe methods organized by theme that encapsultates information on the feature entity, main data and signal type.

The results for a Feature Discovery triggered from an Event Timestamp of a Credit Card transaction table for the Transaction Entity would be as follows:

	Transaction / Credit Card Transaction / Timing:
 		Transaction_Hour_of_Day
		Transaction_Day_of_Week
		Transaction_Month

	Transaction / Credit Card Transaction / Similarity:
	  based on Hour_of_Day
		Transaction_Hour_of_Day_Rel_Freq_vs_Card_Transaction_Count_History
		Transaction_Hour_of_Day_Rel_Freq_vs_Card_Transaction_Amount_History
		Transaction_Hour_of_Day_Rel_Freq_vs_Customer_Transaction_Count_History
		Transaction_Hour_of_Day_Rel_Freq_vs_Customer_Transaction_Amount_History
		Transaction_Hour_of_Day_Rel_Freq_vs_Merchant_Transaction_Count_History
		Transaction_Hour_of_Day_Rel_Freq_vs_Merchant_Transaction_Amount_History
	  based on Day_of_Week
		Transaction_Day_of_Week_Rel_Freq_vs_Card_Transaction_Count_History
		Transaction_Day_of_Week_Rel_Freq_vs_Card_Transaction_Amount_History
		Transaction_Day_of_Week_Rel_Freq_vs_Customer_Transaction_Count_History
		Transaction_Day_of_Week_Rel_Freq_vs_Customer_Transaction_Amount_History
		Transaction_Day_of_Week_Rel_Freq_vs_Merchant_Transaction_Count_History
		Transaction_Day_of_Week_Rel_Freq_vs_Merchant_Transaction_Amount_History

If users choose to extend the search to parent entities, the results would also include:

	Results for Card entity

	Card / Credit Card Transaction / Recency:
		Card_Time_Since_Last_Transaction

	Card / Credit Card Transaction / Timing:
	  based on Hour_of_Day
		Card_Transaction_Count_per_Hour_of_Day
		Card_Transaction_Amount_per_Hour_of_Day
		Card_Hour_of_Day_with_the_Highest_Transaction_Count
		Card_Hour_of_Day_with_the_Highest_Transaction_Amount
		Card_Unique_Count_of_Hour_of_Day
		Card_Entropy_of_Transaction_Count_per_Hour_of_Day
		Card_Entropy_of_Transaction_Amount_per_Hour_of_Day
	  based on Day_of_Week
		Card_Transaction_Count_per_Day_of_Week
		Card_Transaction_Amount_per_Day_of_Week
		Card_Day_of_Week_with_the_Highest_Transaction_Count
		Card_Day_of_Week_with_the_Highest_Transaction_Amount
		Card_Unique_Count_of_Day_of_Week
		Card_Entropy_of_Transaction_Count_per_Day_of_Week
		Card_Entropy_of_Transaction_Amount_per_Day_of_Week
	  based on Inter_Transaction_Time
		Card_Transaction_Clumpiness
		Card_Max_Inter_Transaction_Time

	Card / Credit Card Transaction / Stability:
	  based on Hour_of_Day
		Card_Stability_of_Transaction_Count_per_Hour_of_Day
		Card_Stability_of_Transaction_Amount_per_Hour_of_Day
	  based on Day_of_Week
		Card_Stability_of_Transaction_Count_per_Day_of_Week
		Card_Stability_of_Transaction_Amount_per_Day_of_Week

	Card / Credit Card Transaction / Similarity:
	  based on Hour_of_Day
		Card_Similarity_of_Transaction_Count_per_Hour_of_Day_with_Customer_Cards
		Card_Similarity_of_Transaction_Count_per_Hour_of_Day_with_All_Cards
		Card_Similarity_of_Transaction_Amount_per_Hour_of_Day_with_Customer_Cards
		Card_Similarity_of_Transaction_Amount_per_Hour_of_Day_with_All_Cards
	  based on Day_of_Week
		Card_Similarity_of_Transaction_Count_per_Day_of_Week_with_Customer_Cards
		Card_Similarity_of_Transaction_Count_per_Day_of_Week_with_All_Cards
		Card_Similarity_of_Transaction_Amount_per_Day_of_Week_with_Customer_Cards
		Card_Similarity_of_Transaction_Amount_per_Day_of_Week_with_All_Cards

	Results for Customer entity

	Customer / Credit Card Transaction / Recency:
		Customer_Time_Since_Last_Transaction

	Customer / Credit Card Transaction / Timing:
	  based on Hour_of_Day
		Customer_Transaction_Count_per_Hour_of_Day
		Customer_Transaction_Amount_per_Hour_of_Day
		Customer_Hour_of_Day_with_the_Highest_Transaction_Count
		Customer_Hour_of_Day_with_the_Highest_Transaction_Amount
		Customer_Unique_Count_of_Hour_of_Day
		Customer_Entropy_of_Transaction_Count_per_Hour_of_Day
		Customer_Entropy_of_Transaction_Amount_per_Hour_of_Day
	  based on Day_of_Week
		Customer_Transaction_Count_per_Day_of_Week
		Customer_Transaction_Amount_per_Day_of_Week
		Customer_Day_of_Week_with_the_Highest_Transaction_Count
		Customer_Day_of_Week_with_the_Highest_Transaction_Amount
		Customer_Unique_Count_of_Day_of_Week
		Customer_Entropy_of_Transaction_Count_per_Day_of_Week
		Customer_Entropy_of_Transaction_Amount_per_Day_of_Week
	  based on Inter_Transaction_Time
		Customer_Transaction_Clumpiness
		Customer_Max_Inter_Transaction_Time

	Customer / Credit Card Transaction / Stability:
	  based on Hour_of_Day
		Customer_Stability_of_Transaction_Count_per_Hour_of_Day
		Customer_Stability_of_Transaction_Amount_per_Hour_of_Day
	  based on Day_of_Week
		Customer_Stability_of_Transaction_Count_per_Day_of_Week
		Customer_Stability_of_Transaction_Amount_per_Day_of_Week

	Customer / Credit Card Transaction / Similarity:
	  based on Hour_of_Day
		Customer_Similarity_of_Transaction_Count_per_Hour_of_Day_with_All_Customers
		Customer_Similarity_of_Transaction_Amount_per_Hour_of_Day_with_All_Customers
	  based on Day_of_Week
		Customer_Similarity_of_Transaction_Count_per_Day_of_Week_with_All_Customers
		Customer_Similarity_of_Transaction_Amount_per_Day_of_Week_with_All_Customers

	Results for Merchant entity

	Merchant / Credit Merchant Transaction / Recency:
		Merchant_Time_Since_Last_Transaction

	Merchant / Credit Merchant Transaction / Timing:
	  based on Hour_of_Day
		Merchant_Transaction_Count_per_Hour_of_Day
		Merchant_Transaction_Amount_per_Hour_of_Day
		Merchant_Hour_of_Day_with_the_Highest_Transaction_Count
		Merchant_Hour_of_Day_with_the_Highest_Transaction_Amount
		Merchant_Unique_Count_of_Hour_of_Day
		Merchant_Entropy_of_Transaction_Count_per_Hour_of_Day
		Merchant_Entropy_of_Transaction_Amount_per_Hour_of_Day
	  based on Day_of_Week
		Merchant_Transaction_Count_per_Day_of_Week
		Merchant_Transaction_Amount_per_Day_of_Week
		Merchant_Day_of_Week_with_the_Highest_Transaction_Count
		Merchant_Day_of_Week_with_the_Highest_Transaction_Amount
		Merchant_Unique_Count_of_Day_of_Week
		Merchant_Entropy_of_Transaction_Count_per_Day_of_Week
		Merchant_Entropy_of_Transaction_Amount_per_Day_of_Week
	  based on Inter_Transaction_Time
		Merchant_Transaction_Clumpiness
		Merchant_Max_Inter_Transaction_Time

	Merchant / Credit Merchant Transaction / Stability:
	  based on Hour_of_Day
		Merchant_Stability_of_Transaction_Count_per_Hour_of_Day
		Merchant_Stability_of_Transaction_Amount_per_Hour_of_Day
	  based on Day_of_Week
		Merchant_Stability_of_Transaction_Count_per_Day_of_Week
		Merchant_Stability_of_Transaction_Amount_per_Day_of_Week

	Merchant / Credit Merchant Transaction / Similarity:
	  based on Hour_of_Day
		Merchant_Similarity_of_Transaction_Count_per_Hour_of_Day_with_All_Merchants
		Merchant_Similarity_of_Transaction_Amount_per_Hour_of_Day_with_All_Merchants
	  based on Day_of_Week
		Merchant_Similarity_of_Transaction_Count_per_Day_of_Week_with_All_Merchants
		Merchant_Similarity_of_Transaction_Amount_per_Day_of_Week_with_All_Merchants

To convert the recipes into a feature, users can call the feature recipe method directly from the use case, the view and or the view column.

Help is provided to inform on:

* some required parameters such as the feature window
* the code that could be alternatively used to create the feature in the SDK

### Human-in-the-loop Mode

Feature Discovery combines multiple steps: Joins, Transforms, Subsetting, Aggregation and Post Aggregation Transforms.

Users can choose to decompose those steps and get suggestions at the step level only.

### Discovery Engine

The search for features is based on the data field semantics, the nature of the data and whether the entity is the primary (or natural) key of the data.

The feature recipes are the results of a series of joins, transforms, subsets, aggregations and post aggregation transforms.

* Transform recipes are selected based on the data field semantics and their outputs have new semantics defined by the recipes.
* Subsetting is triggered by the presence of an `Event Type` field in the data.
* Aggregation recipes are selected in function of the nature of the data and entity and the semantics of the field and its transforms.
* Post Aggregation transforms recipes are selected based on the nature of the aggregations

