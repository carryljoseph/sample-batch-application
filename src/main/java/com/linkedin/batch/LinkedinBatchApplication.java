package com.linkedin.batch;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

@SpringBootApplication
@EnableBatchProcessing
public class LinkedinBatchApplication {
	
	private  AWSCredentials credentials = new BasicAWSCredentials(
	          "AKIAZDERS6KM7TQJF4AC", 
	          "mVutuccoKFABLGgsfi9aubnbyBX2r2SlxDycr/w+"
	        );

	private static final Logger logger = LoggerFactory.getLogger(LinkedinBatchApplication.class);

	public static String[] names = new String[] { "orderId", "firstName", "lastName", "email", "cost", "itemId",
			"itemName", "shipDate" };

	public static String ORDER_SQL = "select order_id, first_name, last_name, email, cost, item_id, item_name, ship_date "
			+ "from SHIPPED_ORDER order by order_id";

	public static String[] tokens = new String[] {"order_id", "first_name", "last_name", "email", "cost", "item_id", "item_name", "ship_date"};


	public static String INSERT_ORDER_SQL = "insert into "
			+ "SHIPPED_ORDER_OUTPUT(order_id, first_name, last_name, email, item_id, item_name, cost, ship_date)"
			+ " values(:orderId,:firstName,:lastName,:email,:itemId,:itemName,:cost,:shipDate)";

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	//	@Autowired
	//	public DataSource dataSource;
	//
	//	@Bean
	//	public ItemWriter<Order> itemWriter() {
	//		return new JdbcBatchItemWriterBuilder<Order>()
	//				.dataSource(dataSource)
	//				.sql(INSERT_ORDER_SQL)
	//				.beanMapped()
	//				.build();
	//	}

	@Bean
	public ItemWriter<Order> itemWriter() {
		//		FlatFileItemWriter<Order> itemWriter = new FlatFileItemWriter<Order>();
		S3Resource resource = new S3Resource();
		AmazonStreamWriter<Order> writer = new AmazonStreamWriter<>(resource);
		//		itemWriter.setResource(new FileSystemResource("C:\\Users\\HP\\Documents\\batch_test\\output.txt"));

		DelimitedLineAggregator<Order> aggregator = new DelimitedLineAggregator<Order>();
		aggregator.setDelimiter("|");

		BeanWrapperFieldExtractor<Order> fieldExtractor = new BeanWrapperFieldExtractor<Order>();
		fieldExtractor.setNames(names);
		aggregator.setFieldExtractor(fieldExtractor);

		//		itemWriter.setLineAggregator(aggregator);
		writer.setLineAggregator(aggregator);
		writer.setLineSeparator("\n");
		return writer;
	}

	@Bean
	public ItemReader<Order> itemReader() {
//		String clientRegion = "us-east-2";
//		String roleARN = "arn:aws:iam::625223070361:role/s3oidcAccess";
//		String roleSessionName = "s3-oidc-Access";
		//	        String bucketName = "arn:aws:s3:::cjsamplebatch";

//		AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.standard()
//				.withCredentials(WebIdentityTokenCredentialsProvider.create())
//				.withRegion(clientRegion)
//				.build();

//		AssumeRoleRequest roleRequest = new AssumeRoleRequest()
//				.withRoleArn(roleARN)
//				.withRoleSessionName(roleSessionName);
//		AssumeRoleResult roleResponse = stsClient.assumeRole(roleRequest);
//		Credentials sessionCredentials = roleResponse.getCredentials();

		// Create a BasicSessionCredentials object that contains the credentials you just retrieved.
//		BasicSessionCredentials awsCredentials = new BasicSessionCredentials(
//				sessionCredentials.getAccessKeyId(),
//				sessionCredentials.getSecretAccessKey(),
//				sessionCredentials.getSessionToken());
		logger.info("inside reader");
		

		AmazonS3 s3client = AmazonS3ClientBuilder
//				.defaultClient();
				.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withRegion(Regions.US_EAST_2)
				.build();

		AWSS3Service awsService = new AWSS3Service(s3client);
		StringBuilder str = new StringBuilder();
		String currentdate = new DateTime().toString("yyyyMMdd");
		str.append("input_file/shipped_orders.").append(currentdate);
		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName("cjsamplebatch").withPrefix(str.toString());
		ListObjectsV2Result listing = s3client.listObjectsV2(req);
		S3Object inputS3Obj = s3client.getObject("cjsamplebatch", listing.getObjectSummaries().get(0).getKey());
		
//		S3Object inputS3Obj = awsService.getObject("cjsamplebatch", "input_file/shipped_orders.csv");
		S3ObjectInputStream inputStream = inputS3Obj.getObjectContent();
		Resource resource = new InputStreamResource(inputStream);
		FlatFileItemReader<Order> itemReader = new FlatFileItemReader<Order>();
		itemReader.setResource(resource);
		itemReader.setLinesToSkip(1);
		//		itemReader.setResource(new FileSystemResource("C:\\Users\\HP\\Documents\\batch_test\\shipped_orders.csv"));

		DefaultLineMapper<Order> lineMapper = new DefaultLineMapper<Order>();
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setNames(tokens);

		lineMapper.setLineTokenizer(tokenizer);

		lineMapper.setFieldSetMapper(new OrderFieldSetMapper());

		itemReader.setLineMapper(lineMapper);
		logger.info("exiting reader");
		return itemReader;

	}


	@Bean
	public Step chunkBasedStep() throws Exception {
		return this.stepBuilderFactory.get("chunkBasedStep").<Order, Order>chunk(1000).reader(itemReader())
				.writer(itemWriter())
				.build();
	}

	@Bean
	public Step sftpFileTransferStep() throws Exception {
		return this.stepBuilderFactory.get("sftpFileTransferStep").tasklet(new S3ToSFTPTest()).build();
	}

	@Bean
	public Job job() throws Exception {
		return this.jobBuilderFactory.get("job")
				.start(chunkBasedStep())
				//				.next(sftpFileTransferStep())
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(LinkedinBatchApplication.class, args);
	}


}
