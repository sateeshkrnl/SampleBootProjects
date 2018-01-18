package hello;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.spi.JobFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.SimpleJobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.JobDetailFactoryBean;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	private JobRepository jobRepository;
	
	@Autowired
	private JobLauncher jobLauncher;
	
	@Autowired
	private JobRegistry jobRegistry;
	
	@Autowired
	private JobFactory jobFactory;
	
	@Autowired
	private DataSource dataSource;
	
	@Bean
	public JobFactory jobFactory(ApplicationContext context){
		AutowiringSpringBeanJobFactory jobFactory = new AutowiringSpringBeanJobFactory();
		jobFactory.setApplicationContext(context);
		return jobFactory;
	}
	
	@Bean
	public MapJobRepositoryFactoryBean  mapJobRepositoryFactory() {
	    MapJobRepositoryFactoryBean factoryBean = new MapJobRepositoryFactoryBean(new ResourcelessTransactionManager());
	    try {
	    	
	    	factoryBean.afterPropertiesSet();
	        return factoryBean;
	    } catch (Exception e) {
	        e.printStackTrace();
	        return null;
	    }
	}
	
	@Bean
	public JobBuilderFactory jobBuilderFactory(){
		JobBuilderFactory factory = new JobBuilderFactory(jobRepository);
		return factory;
	}
	
	@Bean
	public StepBuilderFactory stepBuilderFactory(){
		StepBuilderFactory factory = new StepBuilderFactory(jobRepository, new DataSourceTransactionManager(dataSource));
		return factory;
	}
	
	@Bean
	public JobRegistry jobRegistry(){
		return new MapJobRegistry();
	}
	
	@Bean
	public JobRepository jobRepository(MapJobRepositoryFactoryBean factory) throws Exception {
		return factory.getObject();
	}
	
	@Bean
    public JobExplorer jobExplorer(MapJobRepositoryFactoryBean factory) {
        return new SimpleJobExplorer(factory.getJobInstanceDao(), factory.getJobExecutionDao(),
                factory.getStepExecutionDao(), factory.getExecutionContextDao());
    }
	
	@Bean
	public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor() {
		final JobRegistryBeanPostProcessor processor = new JobRegistryBeanPostProcessor();
		processor.setJobRegistry(jobRegistry());
		return processor;
	}

	@Bean
	public JobLauncher jobLauncher(){
		SimpleJobLauncher launcher = new SimpleJobLauncher();
		launcher.setJobRepository(jobRepository);
		launcher.setTaskExecutor(new SimpleAsyncTaskExecutor());
		return launcher;
	}
	
	@Bean
	public Scheduler schedulerFactoryBean(){
		SchedulerFactoryBean obj = new SchedulerFactoryBean();
		Scheduler sch = null; 
		try {
			obj.setJobFactory(jobFactory);
			obj.setTriggers(buildTriggers());

			obj.afterPropertiesSet();
			
			sch = obj.getObject();
			sch.start();
		} catch (Exception e) {
			e.printStackTrace();
		}		
		return sch;
	} 
	
	private CronTrigger[] buildTriggers() {
		CronTriggerFactoryBean factory = new CronTriggerFactoryBean();		
		List<CronTrigger> triggers = new ArrayList<CronTrigger>();
		CronTrigger[] arr = null;
		try {
			factory.setName("quartaz-trigger");
			factory.setCronExpression("0 0/1 * * * ?");
			JobDetail dtl = createJobDetail();
			
			factory.setJobDetail(dtl);
			factory.afterPropertiesSet();
			CronTrigger trigger = factory.getObject();
			triggers.add(trigger);
			arr = new CronTrigger[triggers.size()];
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			if(1){
				e.printStackTrace();
			}
		}
		
		return triggers.toArray(arr);
	}
	
	 
	private JobDetail createJobDetail(){
		JobDetailFactoryBean factory = new JobDetailFactoryBean();
		factory.setName("importUserJob");
		factory.setJobClass(JobLauncherDetails.class);
		factory.setGroup("quartz-batch");
		Map jobDataAsMap = new HashMap<String, Object>();
		jobDataAsMap.put("jobName", "importUserJob");
		jobDataAsMap.put("jobLocator", jobRegistry);
		jobDataAsMap.put("jobLauncher", jobLauncher);
		jobDataAsMap.put("param1", "mkyong");
		factory.setJobDataAsMap(jobDataAsMap);
		factory.afterPropertiesSet();
		return factory.getObject();		
	}
	
	public FlatFileItemReader<Person> reader(){
		FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
		reader.setResource(new ClassPathResource("sample-data.csv"));		
		reader.setLineMapper(new DefaultLineMapper<Person>() {
			{
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames(new String[] { "firstName", "lastName" });
					}
				});
				setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {
					{
						setTargetType(Person.class);
					}
				});
			}
		});
		return reader;
	}
	
	@Bean
	public PersonItemProcessor processor(){
		return new PersonItemProcessor();
	}
	
	@Bean
	public JdbcBatchItemWriter<Person> writer() {
		JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<Person>();
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
		writer.setSql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)");
		writer.setDataSource(dataSource);
		return writer;
	}
	
	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener){
		return jobBuilderFactory.get("importUserJob").incrementer(new RunIdIncrementer())
				.listener(listener)
				.flow(step1()).end().build();
	}
	
	@Bean
	public Step step1(){
		return stepBuilderFactory.get("step1").<Person, Person>chunk(10).reader(reader()).processor(processor())
				.writer(writer()).build();
	}
	
}
