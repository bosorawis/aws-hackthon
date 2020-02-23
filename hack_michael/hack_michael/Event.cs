using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Amazon.Util;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace hack_michael
{
    public class MyEvent
    {
        public string _id;
        public string doctor;
        public string patient;
        public string createdBy;
        public DateTime scheduled;
        public string message;
        public string _type;

        static MyEvent[] testEvents = new[] {
            new MyEvent() { doctor="Dr. Smith", patient="John Doe, john.doe@gmail.com",createdBy="Nurse Ratchet", scheduled = DateTime.Now.AddMinutes(1.0), _type = "event" },
            new MyEvent() { doctor="Dr. Smith", patient="Jane Doe, john.doe@gmail.com",createdBy="Nurse Ratchet", scheduled = DateTime.Now.AddMinutes(2.0), _type = "event" },
            new MyEvent() { doctor="Dr. Smith", patient="Jimmy Jones, john.doe@gmail.com",createdBy="Dr. Smith", scheduled = DateTime.Now.AddMinutes(1.2), _type = "event" },
            new MyEvent() { doctor="Dr. Spoc", patient="Little Timmy, john.doe@gmail.com",createdBy="Nurse Ratchet", scheduled = DateTime.Now.AddMinutes(0.1), _type = "event" },
            new MyEvent() { doctor="Dr. Smith", patient="John Doe, john.doe@gmail.com",createdBy="Nurse Sam", scheduled = DateTime.Now.AddMinutes(3.0), _type = "event"  }
        };
        public Dictionary<string, AttributeValue>  GetAttributes()
        {
            Guid guid = Guid.NewGuid();
            
            Dictionary<string, AttributeValue> attributes = new Dictionary<string, AttributeValue>();
           attributes["_id"] = new AttributeValue { S = guid.ToString() };
            // Title is range-key
            attributes["publisher"] = new AttributeValue { S = doctor };
            // Other attributes

            attributes["subscriber"] = new AttributeValue { S = patient };
            attributes["createdBy"] = new AttributeValue { S = createdBy };

            attributes["body"] = new AttributeValue { S = message };
            //{
            //    SS = new List<string> { "Please come to the appointment for " +  doctor + " at " + scheduled.ToLongDateString(), "ATT:503-809-1602" }
            //};
            attributes["type"] = new AttributeValue { S = _type };
            attributes["seq"] = new AttributeValue { N = "1" };

            int epochSeconds = AWSSDKUtils.ConvertToUnixEpochSeconds(DateTime.Now.AddMinutes(0.5).ToUniversalTime());

           attributes["scheduled"] = new AttributeValue { N = epochSeconds.ToString() };

            return attributes;
        }
        public async Task Put(string tableName)
        {
            // Create a client
            AmazonDynamoDBClient client = new AmazonDynamoDBClient();

            // Define item attributes
            Dictionary<string, AttributeValue> attributes = GetAttributes();

            // Create PutItem request
            PutItemRequest request = new PutItemRequest
            {
                TableName = tableName,
                Item = attributes
            };

            // Issue PutItem request
            await client.PutItemAsync(request);
        }

        static public async Task<bool> UnitTest()
        {
            try
            {
                foreach (MyEvent testEvent in testEvents)
                {
                    await testEvent.Put("naggingEvent");
                }
            }
            catch (Exception exc)
            {
                Console.WriteLine(exc.ToString());
                return false;
            }
            return true;
        }

        private string ToString()
        {
            string result = "";
            result = doctor.ToString();
            return result;
        }
        private void ToFile()
        {
            
                string errorFilename = "C:/Program Files/AutoMap/Errors/hack.txt";

                System.IO.StreamWriter strm = System.IO.File.AppendText(errorFilename);

                strm.WriteLine(ToString());
                strm.Flush();
                strm.Close();
            
        }
        public class LambdaRequest
        {
            public string body { get; set; }
        }

        [LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]
            public async Task<APIGatewayProxyResponse> FunctionHandler(APIGatewayProxyRequest  request  /*MyEvent request  */, ILambdaContext context)            
            {

            Console.WriteLine("Got call to MyEvnet");
            if (null == request)
                Console.WriteLine("request = null");
            Console.WriteLine("req=" + request.Body);
            MyEvent myEvent = JsonConvert.DeserializeObject<MyEvent>(request.Body);


            //    if (string.IsNullOrWhiteSpace(request.doctor))
            //        Console.WriteLine("doctor is null");
            //    else
            //    Console.WriteLine("doctor=" + request.doctor);
            //   // UnitTest();
            ////     ToFile();
            await myEvent.Put("naggingEvent");
            APIGatewayProxyResponse ret = new APIGatewayProxyResponse();
            ret.StatusCode = 200;
            return ret;
            }

            //[LambdaSerializer(typeof(MyJsonSerializer))]
            //public Customer DescribeCustomer(DescribeCustomerRequest request)
            //{
            //    return customerService.DescribeCustomer(request.Id);
            //}
        

    }
}
