package com.linkedin.batch;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder;
import org.springframework.batch.item.validator.BeanValidatingItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@SpringBootApplication
@EnableBatchProcessing
public class LinkedinBatchApplicationRef {

	public static String[] tokens = new String[] {"order_id", "first_name", "last_name", "email", "cost", "item_id", "item_name", "ship_date"};
	
	public static String INSERT_ORDER_SQL = "insert into "
			+ "SHIPPED_ORDER_OUTPUT(order_id, first_name, last_name, email, item_id, item_name, cost, ship_date, tracking_number, free_shipping)"
			+ " values(:orderId,:firstName,:lastName,:email,:itemId,:itemName,:cost,:shipDate,:trackingNumber,:freeShipping)";

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;
	
	@Bean
	public ItemProcessor<TrackedOrder,TrackedOrder> freeShippingItemProcessor() {
		return new FreeShippingItemProcessor();
	}
	
	@Bean
	public ItemProcessor<Order,TrackedOrder> compositeItemProcessor() {
		return new CompositeItemProcessorBuilder<Order, TrackedOrder>()
				.delegates(orderValidatingItemProcessor(),trackedOrderItemProcessor(),freeShippingItemProcessor())
				.build();
	}
	
	@Bean
	public ItemProcessor<Order,Order> orderValidatingItemProcessor() {
		BeanValidatingItemProcessor<Order> itemProcessor = new BeanValidatingItemProcessor<Order>();
		itemProcessor.setFilter(true);
		return itemProcessor;
	}
	

	@Bean
	public ItemProcessor<Order,TrackedOrder> trackedOrderItemProcessor() {
		return new TrackedOrderItemProcessor();
	}

	@Bean
	public ItemWriter<TrackedOrder> itemWriter() {
		return new JdbcBatchItemWriterBuilder<TrackedOrder>()
				.dataSource(dataSource)
				.sql(INSERT_ORDER_SQL)
				.beanMapped()
//				.itemPreparedStatementSetter(new OrderItemPreparedStatementSetter())
				.build();
	}

	@Bean
	public ItemReader<Order> itemReader() {
		FlatFileItemReader<Order> itemReader = new FlatFileItemReader<Order>();
		itemReader.setLinesToSkip(1);
		itemReader.setResource(new FileSystemResource("C:\\Users\\HP\\Downloads\\Ex_Files_Spring_Spring_Batch\\Exercise Files\\data\\shipped_orders.csv"));
		
		DefaultLineMapper<Order> lineMapper = new DefaultLineMapper<Order>();
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setNames(tokens);
		
		lineMapper.setLineTokenizer(tokenizer);
		
		lineMapper.setFieldSetMapper(new OrderFieldSetMapper());
		
		itemReader.setLineMapper(lineMapper);
		
		itemReader.setSaveState(false);
		
		return itemReader;
		
	}
	
	@Bean
	public TaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(2);
		executor.setMaxPoolSize(10);
		return executor;
	}

	@Bean
	public Step chunkBasedStep() throws Exception {
		return this.stepBuilderFactory.get("chunkBasedStep")
				.<Order, TrackedOrder>chunk(10)
				.reader(itemReader())
				.processor(compositeItemProcessor())	
				.faultTolerant()
				.retry(OrderProcessingException.class)
				.retryLimit(3)
				.listener(new CustomRetryListener())
				.writer(itemWriter())
				.taskExecutor(taskExecutor())
				.build();
	}



	@Bean
	public Job job() throws Exception {
		return this.jobBuilderFactory.get("job").start(chunkBasedStep()).build();
	}

	public static void main(String[] args) {
		SpringApplication.run(LinkedinBatchApplicationRef.class, args);
	}

}
