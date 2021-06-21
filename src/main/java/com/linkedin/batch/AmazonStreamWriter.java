package com.linkedin.batch;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.WritableResource;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.WebIdentityTokenCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;

public class AmazonStreamWriter<Order> implements ItemWriter<Order>,ItemWriteListener<Order>{
	
	
    private WritableResource resource;
    private DelimitedLineAggregator<Order> lineAggregator;
    private String lineSeparator;

    public String getLineSeparator() {
        return lineSeparator;
    }

    public void setLineSeparator(String lineSeparator) {
        this.lineSeparator = lineSeparator;
    }

    AmazonStreamWriter(WritableResource resource){
        this.resource = resource;
    }

    public WritableResource getResource() {
        return resource;
    }

    public void setResource(WritableResource resource) {
        this.resource = resource;
    }

    public DelimitedLineAggregator<Order> getLineAggregator() {
        return lineAggregator;
    }

    public void setLineAggregator(DelimitedLineAggregator<Order> lineAggregator) {
        this.lineAggregator = lineAggregator;
    }

    @Override
    public void write(List<? extends Order> items) throws Exception {
        try (OutputStream outputStream = resource.getOutputStream()) {
                StringBuilder lines = new StringBuilder();
                Iterator var3 = items.iterator();

                while(var3.hasNext()) {
                    Order item = (Order) var3.next();
                    lines.append(this.lineAggregator.aggregate(item)).append(this.lineSeparator);
                }
                outputStream.write(lines.toString().getBytes());
        }
    }

	@Override
	public void beforeWrite(List<? extends Order> items) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void afterWrite(List<? extends Order> items) {
//		String clientRegion = "us-east-2";
//        String roleARN = "arn:aws:iam::625223070361:role/s3oidcAccess";
//        String roleSessionName = "s3-oidc-Access";
//
//	AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.standard()
//			.withCredentials(WebIdentityTokenCredentialsProvider.create())
//			.withRegion(clientRegion)
//			.build();
//
//	AssumeRoleRequest roleRequest = new AssumeRoleRequest()
//			.withRoleArn(roleARN)
//			.withRoleSessionName(roleSessionName);
//	AssumeRoleResult roleResponse = stsClient.assumeRole(roleRequest);
//	Credentials sessionCredentials = roleResponse.getCredentials();

	// Create a BasicSessionCredentials object that contains the credentials you just retrieved.
//	BasicSessionCredentials awsCredentials = new BasicSessionCredentials(
//			sessionCredentials.getAccessKeyId(),
//			sessionCredentials.getSecretAccessKey(),
//			sessionCredentials.getSessionToken());

		 AmazonS3 s3client = AmazonS3ClientBuilder.defaultClient();
//		          .standard()
//		          .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
//		          .withRegion(Regions.US_EAST_2)
//		          .build();
	    try {
			s3client.putObject(new com.amazonaws.services.s3.model.PutObjectRequest("cjsamplebatch", "output_file/output.txt", this.getResource().getInputStream(),new ObjectMetadata()));
	    } catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onWriteError(Exception exception, List<? extends Order> items) {
		// TODO Auto-generated method stub
		
	}
}