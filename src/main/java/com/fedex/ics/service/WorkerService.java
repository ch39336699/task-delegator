package com.fedex.ics.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class WorkerService {
  ArrayList<String> taskPlans = new ArrayList<String>();

  private static final Log logger = LogFactory.getLog(WorkerService.class);

  public WorkerService() {
  }

  public void kickOffTaskPlan(ArrayList<String> parameters) {
    ArrayList<String> taskPlans = getTaskPlans();
    try {
      for (int count = 0; count < taskPlans.size(); count++) {
        if(taskPlans.get(count).contains(parameters.get(0)))
        {
        getHTML("http://localhost:9393/streams/deployments/" + taskPlans.get(count) + "/" + parameters.get(1));
          return;
        }
      }
      logger.info("WARNING: NO TASK PLAN MATCHED FOR REGION: " + parameters.get(0));
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void doWork(String message) {
    ArrayList<String> parameters = new ArrayList<String>();
    String[] values = message.split(",");
    if (values != null && values.length > 1) {
      for (int count = 0; count < values.length; count++) {
        String[] values2 = values[count].split(":");
        if (values != null && values.length > 1) {
        parameters.add(values2[1]);
        }
      }
      if(parameters != null && parameters.size() > 1)
      {
        kickOffTaskPlan(parameters);
      }
      else
      {
        logger.info("WARNING: NO TASK PLAN MATCHED FOR message: " + message);
      }
    }
    else
    {
      logger.info("WARNING: NO TASK PLAN MATCHED FOR message: " + message);
    }
  }

  public ArrayList<String> getTaskPlans() {
    ArrayList<String> taskPlans = new ArrayList<String>();
    try {
      String result = getHTML("http://localhost:9393/streams/definitions");
      String[] tempArray = result.split(",");
      for (int count = 0; count < tempArray.length; count++) {
        if (tempArray[count].contains("taskName")) {
          String[] tempArray2 = tempArray[count].split(":");
          taskPlans.add(tempArray2[1].substring(1, tempArray2[1].length() - 1));
        }
      }
    } catch (Exception e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    return taskPlans;
  }

  public static String getHTML(String urlToRead) throws Exception {
    StringBuilder result = new StringBuilder();
    URL url = new URL(urlToRead);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    String line;
    while ((line = rd.readLine()) != null) {
      result.append(line);
    }
    rd.close();
    return result.toString();
  }
}
