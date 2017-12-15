This is a key-value producer.  At the prompt, enter a comma-separated (key, value) string.  Both values are considered strings and sent to topic 'input-topic'.  (To override the default topic name, specify it on the command line.)

Input example: 
    1, A
    2, {"some":"json", "more":"json"}
    

You can safely enter JSON for the value.  The key is considered all the text before the first comma in the input.


