package com.example.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

public class SqsToSnsLambda implements RequestHandler<SQSEvent, String> {

    private final SnsClient snsClient = SnsClient.create();
    private static final String SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:422032868698:module8-UploadsNotificationTopic";

    @Override
    public String handleRequest(SQSEvent event, Context context) {
        for (SQSEvent.SQSMessage message : event.getRecords()) {
            String body = message.getBody();
            context.getLogger().log("Received from SQS: " + body);

            snsClient.publish(PublishRequest.builder()
                    .topicArn(SNS_TOPIC_ARN)
                    .message(body)
                    .build());

            context.getLogger().log("Published to SNS.");
        }
        return "SUCCESS";
    }
}
