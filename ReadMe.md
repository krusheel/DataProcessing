This application reads tweets from a files and extracts youtube urls and their statistics present in the tweets 

Input: A file containing tweets in json format
Output: A json containing youtube video ids along with their statistics in sorted order based on the most number of likes

Creating jar file
1. Add your YOUTUBE data api key in YouTubeVideoStatistics.java to the variable apiKey in line 39.
2. Goto root of the project folder. Let us call this DATAPROCESSING_PATH.
3. Run gradle clean assemble
4. jar file will be available in  DATAPROCESSING_PATH/build/libs

Run Command
java -jar DataProcessing-1.0.jar <tweets_json_filename>