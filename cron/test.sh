#!/bin/bash

while true;
do
aws lambda invoke --function-name test123-HelloWorldFunction-1FWP8O3BOOCHC ---region us-west-1 --profile ht outfile

sleep 5;
done