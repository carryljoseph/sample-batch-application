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
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

public class AmazonStreamWriter<Order> implements ItemWriter<Order>,ItemWriteListener<Order>{
	
	private AWSCredentials credentials = new BasicAWSCredentials(
	          "AKIAZDERS6KM7TQJF4AC", 
	          "mVutuccoKFABLGgsfi9aubnbyBX2r2SlxDycr/w+"
	        );
	
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
		 AmazonS3 s3client = AmazonS3ClientBuilder
		          .standard()
		          .withCredentials(new AWSStaticCredentialsProvider(credentials))
		          .withRegion(Regions.US_EAST_2)
		          .build();
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