package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/aws/aws-sdk-go/service/sns"
)

var table = os.Getenv("TABLE")
var sess = session.New()
var dynamoSvc = dynamodb.New(sess)
var snsSvc = sns.New(sess)
var typeStr = "event"
var snsTopic = "arn:aws:sns:us-west-1:400794274921:%s"
var maxSequence = 3
var multiplier = 180

type event struct {
	ID         string   `json:"_id,string,omitempty"`
	Publisher  string   `json:"publisher,string,omitempty"`
	Subscriber string   `json:"subscriber,string,omitempty"`
	CreatebBy  string   `json:"createdBy,string,omitempty"`
	Scheduled  int64    `json:"scheduled,int64,omitempty"`
	Body       []string `json:"body,string,omitempty"`
	Type       string   `json:"type,string,omitempty"`
	Medone     string   `json:"medone,string,omitempty"`
	Sequence   string   `json:"seq,string,omitempty"`
}

func handler() error {
	currentTIme := time.Now().Unix()
	keyCond := expression.Key("type").Equal(expression.Value(typeStr))
	nextKey := expression.Key("scheduled").LessThan(expression.Value(currentTIme))
	processed := expression.Name("medone").AttributeNotExists()
	exp, err := expression.NewBuilder().WithKeyCondition(keyCond.And(nextKey)).WithCondition((processed)).Build()

	if err != nil {
		fmt.Println("failed to build expression")
		return err
	}
	params := &dynamodb.QueryInput{
		TableName:                 aws.String(table),
		IndexName:                 aws.String("type-scheduled-index"),
		KeyConditionExpression:    exp.KeyCondition(),
		ExpressionAttributeValues: exp.Values(),
		FilterExpression:          exp.Condition(),
		ExpressionAttributeNames:  exp.Names(),
		ConsistentRead:            aws.Bool(false),
		Limit:                     aws.Int64(100),
	}

	result, err := dynamoSvc.Query(params)
	if err != nil {
		fmt.Println(err)
		return err
	}

	for _, data := range result.Items {
		item := &event{}
		fmt.Println("hello")
		fmt.Println(data)
		err = dynamodbattribute.UnmarshalMap(data, item)
		if err != nil {
			fmt.Println(err)
		}

		input := &sns.PublishInput{
			Message:  aws.String(fmt.Sprintf("Publisher: %s \n Reminder Number: #%s Body: %s", item.Publisher, item.Sequence, item.Body)),
			TopicArn: aws.String(fmt.Sprintf(snsTopic, item.Subscriber)),
		}
		_, err = snsSvc.Publish(input)
		if err != nil {
			fmt.Println(err)
		} else {
			updateItem(item)
		}
	}

	return nil
}

func updateItem(e *event) {
	currentSequence, _ := strconv.Atoi(e.Sequence)
	seq := maxSequence - currentSequence

	currentSequence = currentSequence + 1
	timeToAdd := 180 - (seq * 30)
	nexttime := time.Now().Add(time.Duration(timeToAdd) * time.Second).Unix()
	timeStamp := strconv.FormatInt(nexttime, 10)

	var putParam *dynamodb.UpdateItemInput
	if seq <= 0 {
		putParam = &dynamodb.UpdateItemInput{
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":r": {
					S: aws.String("done"),
				},
			},
			TableName: aws.String(table),
			Key: map[string]*dynamodb.AttributeValue{
				"_id": {
					S: aws.String(e.ID),
				},
			},
			ReturnValues:     aws.String("UPDATED_NEW"),
			UpdateExpression: aws.String("set medone = :r"),
		}
	} else {
		putParam = &dynamodb.UpdateItemInput{
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				// ":r": {
				// 	S: aws.String("done"),
				// },
				":s": {
					S: aws.String(strconv.Itoa(currentSequence)),
				},
				":t": {
					N: aws.String(timeStamp),
				},
			},
			TableName: aws.String(table),
			Key: map[string]*dynamodb.AttributeValue{
				"_id": {
					S: aws.String(e.ID),
				},
			},
			ReturnValues:     aws.String("UPDATED_NEW"),
			UpdateExpression: aws.String("set scheduled = :t, seq = :s"),
		}
	}

	_, err := dynamoSvc.UpdateItem(putParam)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func main() {
	lambda.Start(handler)
}
