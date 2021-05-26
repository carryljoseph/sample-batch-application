package com.linkedin.batch;

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
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

@SpringBootApplication
@EnableBatchProcessing
public class LinkedinBatchApplication {
	
	private AWSCredentials credentials = new BasicAWSCredentials(
	          "AKIAZDERS6KM7TQJF4AC", 
	          "mVutuccoKFABLGgsfi9aubnbyBX2r2SlxDycr/w+"
	        );

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
		AmazonS3 s3client = AmazonS3ClientBuilder
		          .standard()
		          .withCredentials(new AWSStaticCredentialsProvider(credentials))
		          .withRegion(Regions.US_EAST_2)
		          .build();
		        
	    AWSS3Service awsService = new AWSS3Service(s3client);
	    S3Object inputS3Obj = awsService.getObject("cjsamplebatch", "input_file/shipped_orders.csv");
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
		return itemReader;
		
	}
	

	@Bean
	public Step chunkBasedStep() throws Exception {
		return this.stepBuilderFactory.get("chunkBasedStep").<Order, Order>chunk(1000).reader(itemReader())
				.writer(itemWriter()).build();
	}

	@Bean
	public Job job() throws Exception {
		return this.jobBuilderFactory.get("job").start(chunkBasedStep()).build();
	}

	public static void main(String[] args) {
		SpringApplication.run(LinkedinBatchApplication.class, args);
	}
	

}
