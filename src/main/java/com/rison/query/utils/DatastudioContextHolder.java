package com.rison.query.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * @PACKAGE_NAME: com.rison.query.utils
 * @NAME: DatastudioContextHolder
 * @USER: Rison
 * @DATE: 2022/10/29 0:43
 * @PROJECT_NAME: bigdata-query-handler
 **/
@Component
public class DatastudioContextHolder implements ApplicationContextAware {
    private static ApplicationContext context = null;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }

    public static ApplicationContext getApplicationContext() {
        return context;
    }

    public static <T> T getBean(Class<T> type) {
        return (T)getApplicationContext().getBean(type);
    }

    public static <T> T getBean(String name) {
        return (T)context.getBean(name);
    }
}
