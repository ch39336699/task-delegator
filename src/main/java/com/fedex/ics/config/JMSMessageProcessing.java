package com.fedex.ics.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.jms.ConnectionFactory;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.fedex.ics.service.WorkerService;
import com.fedex.mi.decorator.jms.FedexJmsConnectionFactory;

@Configuration
public class JMSMessageProcessing  {

  private static final Log logger = LogFactory.getLog(JMSMessageProcessing.class);

  //@Value("${kafka.bootstrapAddress}")
  private String bootstrapServers;

  //@Value("${aim.concurrency}")
  private String concurrency = "1";
 // @Value("${aim.factoryName}")
  private String factoryName = "fxClientUID=S.MIB-INT.US.CL02.EMS06.M.L1";
  
  @Autowired
  private WorkerService worker;
  
  //@Value("${aim.ldap.queueUserName}")
  private String ldapUserName = "FCSC-CS-CLEARANCE-CORP-5710";
  //@Value("${aim.ldap.queuePassword}")
  private String ldapPassword = "7QMYcNkjJigAWo9HHQoym9Mdd";

 // @Value("${directory.ldapUrl}")
  private String ldapUrl = "ldap://apptstldap.corp.fedex.com:389/ou=messaging,dc=corp,dc=fedex,dc=com";

  @Bean(name = "jmsMessageFactory")
  public JmsListenerContainerFactory<DefaultMessageListenerContainer> aimMessageFactory() {
    DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
    factory.setConnectionFactory(jmsConnectionFactory());
    factory.setConcurrency(concurrency);
    factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
    factory.setSessionTransacted(true);
    return factory;
  }

  @Bean(name = "jmsConnectionFactory")
  public ConnectionFactory jmsConnectionFactory() {
    FedexJmsConnectionFactory connectionFactory = null;
    try {
      Properties props = new Properties();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "com.fedex.mi.decorator.jms.FedexTibcoInitialContext");
      props.put(Context.PROVIDER_URL, ldapUrl);
      props.put(Context.SECURITY_PRINCIPAL, ldapUserName);
      props.put(Context.SECURITY_CREDENTIALS, "7QMYcNkjJigAWo9HHQoym9Mdd");

      InitialContext ctx;
      ctx = new InitialContext(props);
      connectionFactory = (FedexJmsConnectionFactory) ctx.lookup(factoryName);
      connectionFactory.setUser("FCSC-CS-CLEARANCE-CORP-5710");
      connectionFactory.setPassword(ldapPassword);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return connectionFactory;
  }

  @JmsListener(destination = "FDXIPC.CIM.RETRYREGFROMCUST.USA", containerFactory = "jmsMessageFactory")
  public void recieveMessage(javax.jms.Message message) {
    //TODO: check JMS retry counts
    try {
      String messageBody = message.getBody(String.class);
      logger.info("Recieved Message: " + messageBody);
      worker.doWork(messageBody);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Bean("jmsProducerConfigs")
  public Map<String, Object> jmsProducerConfigs() {
    Map<String, Object> props = new HashMap<>();
    // list of host:port pairs used for establishing the initial connections to the Kakfa cluster
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    return props;
  }
}
