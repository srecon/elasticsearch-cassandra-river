package com.blu.es.job;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;


/**
 * Created by samim on 09/07/14.
 */
public class JobTest {
    public static void main(String[] args) throws Exception{
        System.out.println("Test Quartz Schedular");
        JobDetail jobDetail = JobBuilder.newJob(RiverJob.class).withIdentity("River Job","river").build();
        //trigger
        Trigger trigger = TriggerBuilder.newTrigger()
                                        .withIdentity("RiverTrigger", "river")
                                        .withSchedule(CronScheduleBuilder.cronSchedule("0/5 * * * * ?"))
                                        .build();
        // schedule
        Scheduler scheduler = new StdSchedulerFactory().getScheduler();
        scheduler.start();
        scheduler.scheduleJob(jobDetail, trigger);
    }
}
