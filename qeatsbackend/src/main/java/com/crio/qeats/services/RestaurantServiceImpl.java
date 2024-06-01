
/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.services;

import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.exchanges.GetRestaurantsRequest;
import com.crio.qeats.exchanges.GetRestaurantsResponse;
import com.crio.qeats.repositoryservices.RestaurantRepositoryService;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;

@Service
@Log4j2
public class RestaurantServiceImpl implements RestaurantService {
  

  private final Double peakHoursServingRadiusInKms = 3.0;
  private final Double normalHoursServingRadiusInKms = 5.0;
  @Autowired
  private RestaurantRepositoryService restaurantRepositoryService;


  // TODO: CRIO_TASK_MODULE_RESTAURANTSAPI - Implement findAllRestaurantsCloseby.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findAllRestaurantsCloseBy(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
        // System.out.print(getRestaurantsRequest);

        List<Restaurant> restaurant;
        boolean isPeek = inBetween(currentTime);

 
        if (isPeek) {
          restaurant = restaurantRepositoryService.findAllRestaurantsCloseBy(
              getRestaurantsRequest.getLatitude(), getRestaurantsRequest.getLongitude(), 
              currentTime, peakHoursServingRadiusInKms);
        } else {
          restaurant = restaurantRepositoryService.findAllRestaurantsCloseBy(
            getRestaurantsRequest.getLatitude(), getRestaurantsRequest.getLongitude(), 
            currentTime, normalHoursServingRadiusInKms);
        }
        GetRestaurantsResponse response = new GetRestaurantsResponse(restaurant);
        log.info(response);
        return response;
    

  }


  public boolean inBetween(LocalTime currentTime) {

    if (currentTime.isAfter(LocalTime.of(7, 59)) && currentTime.isBefore(LocalTime.of(10, 01))
        || currentTime.isAfter(LocalTime.of(12, 59)) && currentTime.isBefore(LocalTime.of(14, 01))
        || currentTime.isAfter(LocalTime.of(18, 59)) 
        && currentTime.isBefore(LocalTime.of(21, 01))) {
      return true;
    }

    return false;
  }


  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Implement findRestaurantsBySearchQuery. The request object has the search string.
  // We have to combine results from multiple sources:
  // 1. Restaurants by name (exact and inexact)
  // 2. Restaurants by cuisines (also called attributes)
  // 3. Restaurants by food items it serves
  // 4. Restaurants by food item attributes (spicy, sweet, etc)
  // Remember, a restaurant must be present only once in the resulting list.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQuery(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {

 
        HashSet<Restaurant> hs = new LinkedHashSet<Restaurant>();
        List<Restaurant> resName = new ArrayList<Restaurant>();
    
        if (getRestaurantsRequest.getSearchFor() != null 
            && getRestaurantsRequest.getSearchFor().equals("")) {
    
          GetRestaurantsResponse getRestaurantsResponse = new GetRestaurantsResponse();
          getRestaurantsResponse.setRestaurants(new ArrayList<Restaurant>());
          return getRestaurantsResponse;
        }
    
        List<Restaurant> temp = new ArrayList<Restaurant>();

        boolean isPeek = inBetween(currentTime);

 
        if (isPeek) {
                temp = restaurantRepositoryService.findRestaurantsByName(getRestaurantsRequest.getLatitude(),
          getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
          peakHoursServingRadiusInKms);

      callUnique(hs, resName, temp);

      temp = restaurantRepositoryService.findRestaurantsByAttributes(
          getRestaurantsRequest.getLatitude(),
          getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
          peakHoursServingRadiusInKms);

      callUnique(hs, resName, temp);

      temp = restaurantRepositoryService.findRestaurantsByItemName(
          getRestaurantsRequest.getLatitude(),
          getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
          peakHoursServingRadiusInKms);

      callUnique(hs, resName, temp);

      temp = restaurantRepositoryService.findRestaurantsByItemAttributes(
          getRestaurantsRequest.getLatitude(),
          getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
          peakHoursServingRadiusInKms);

      callUnique(hs, resName, temp);;
        }
        
        
        
        else {
          temp = restaurantRepositoryService.findRestaurantsByName(getRestaurantsRequest.getLatitude(),
          getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
          normalHoursServingRadiusInKms);

      callUnique(hs, resName, temp);

      temp = restaurantRepositoryService.findRestaurantsByAttributes(
          getRestaurantsRequest.getLatitude(),
          getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
          normalHoursServingRadiusInKms);

      callUnique(hs, resName, temp);

      temp = restaurantRepositoryService.findRestaurantsByItemName(
          getRestaurantsRequest.getLatitude(),
          getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
          normalHoursServingRadiusInKms);

      callUnique(hs, resName, temp);

      temp = restaurantRepositoryService.findRestaurantsByItemAttributes(
          getRestaurantsRequest.getLatitude(),
          getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
          normalHoursServingRadiusInKms);

      callUnique(hs, resName, temp);
        }
        for (Restaurant r : hs) {
          resName.add(r);
        }
    
        GetRestaurantsResponse getRestaurantsResponse = new GetRestaurantsResponse();
        getRestaurantsResponse.setRestaurants(resName);
    
        return getRestaurantsResponse;  }



        static void callUnique(HashSet<Restaurant> hs, List<Restaurant> res, List<Restaurant> temp) {

          if (temp == null || temp.size() == 0) {
            return;
          }
      
          for (Restaurant r : temp) {
            hs.add(r);
          }
      
        }
  

  // TODO: CRIO_TASK_MODULE_MULTITHREADING
  // Implement multi-threaded version of RestaurantSearch.
  // Implement variant of findRestaurantsBySearchQuery which is at least 1.5x time faster than
  // findRestaurantsBySearchQuery.
  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQueryMt(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {


        GetRestaurantsResponse getRestaurantsResponse = new GetRestaurantsResponse();
        HashSet<Restaurant> hs = new LinkedHashSet<Restaurant>();
        List<Restaurant> resName = new ArrayList<Restaurant>();
    
        if (getRestaurantsRequest.getSearchFor() != null 
            && getRestaurantsRequest.getSearchFor().equals("")) {
          getRestaurantsResponse.setRestaurants(new ArrayList<Restaurant>());
          return getRestaurantsResponse;
        }
    
        
        Future<List<Restaurant>> temp1 = 
            findRestaurantsByNameAsyn(getRestaurantsRequest, currentTime);
    
        Future<List<Restaurant>> temp2 = findRestaurantsByAttributesAsyn(
            getRestaurantsRequest, currentTime);
    
        Future<List<Restaurant>> temp3 = findRestaurantsByItemNameAsyn(
            getRestaurantsRequest, currentTime);
    
        Future<List<Restaurant>> temp4 = findRestaurantsByItemAttributesAsyn(
            getRestaurantsRequest, currentTime);
    
         
    
        try {
          for (Restaurant res : temp1.get()) {
            hs.add(res);
          }
        } catch (InterruptedException | ExecutionException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
          
        try {
          for (Restaurant res : temp2.get()) {
            hs.add(res);
          }
        } catch (InterruptedException | ExecutionException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        try {
          for (Restaurant res : temp3.get()) {
            hs.add(res);
          }
        } catch (InterruptedException | ExecutionException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        try {
          for (Restaurant res : temp4.get()) {
            hs.add(res);
          }
        } catch (InterruptedException | ExecutionException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }    
    
        for (Restaurant res : hs) {
          resName.add(res);
        }
        
        getRestaurantsResponse.setRestaurants(resName);
    
        return getRestaurantsResponse;  }
        @Async
        Future<List<Restaurant>> findRestaurantsByNameAsyn(
            GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
      
          ExecutorService executorService = Executors.newFixedThreadPool(20);
      
          if (!inBetween(currentTime)) {
            return (Future<List<Restaurant>>) executorService.submit(() -> {
              restaurantRepositoryService.findRestaurantsByItemAttributes(
                  getRestaurantsRequest.getLatitude(),
                  getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
                  normalHoursServingRadiusInKms);
            });
          } else {
            return (Future<List<Restaurant>>) executorService.submit(() -> {
              restaurantRepositoryService.findRestaurantsByItemAttributes(
                  getRestaurantsRequest.getLatitude(),
                  getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
                  peakHoursServingRadiusInKms);
            });
          }          
        }
        
        @Async
        Future<List<Restaurant>> findRestaurantsByAttributesAsyn(
            GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
      
          ExecutorService executorService = Executors.newFixedThreadPool(20);
      
          if (!inBetween(currentTime)) {
            return (Future<List<Restaurant>>) executorService.submit(() -> {
              restaurantRepositoryService.findRestaurantsByAttributes(
                  getRestaurantsRequest.getLatitude(),
                  getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
                  normalHoursServingRadiusInKms);
            });
          } else {
            return (Future<List<Restaurant>>) executorService.submit(() -> {
              restaurantRepositoryService.findRestaurantsByAttributes(
                  getRestaurantsRequest.getLatitude(),
                  getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
                  peakHoursServingRadiusInKms);
            });
          }          
        }
      
        @Async
        Future<List<Restaurant>> findRestaurantsByItemNameAsyn(
      
            GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
      
          ExecutorService executorService = Executors.newFixedThreadPool(20);
      
          if (!inBetween(currentTime)) {
            return (Future<List<Restaurant>>) executorService.submit(() -> {
              restaurantRepositoryService.findRestaurantsByItemName(
                  getRestaurantsRequest.getLatitude(),
                  getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
                  normalHoursServingRadiusInKms);
            });
          } else {
            return (Future<List<Restaurant>>) executorService.submit(() -> {
              restaurantRepositoryService.findRestaurantsByItemName(
                  getRestaurantsRequest.getLatitude(),
                  getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
                  peakHoursServingRadiusInKms);
            });
          }          
        }
      
      
      
        @Async
        Future<List<Restaurant>> findRestaurantsByItemAttributesAsyn(
      
            GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
      
          ExecutorService executorService = Executors.newFixedThreadPool(20);
      
          if (!inBetween(currentTime)) {
            return (Future<List<Restaurant>>) executorService.submit(() -> {
              restaurantRepositoryService.findRestaurantsByItemAttributes(
                  getRestaurantsRequest.getLatitude(),
                  getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
                  normalHoursServingRadiusInKms);
            });
          } else {
            return (Future<List<Restaurant>>) executorService.submit(() -> {
              restaurantRepositoryService.findRestaurantsByItemAttributes(
                  getRestaurantsRequest.getLatitude(),
                  getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), currentTime,
                  peakHoursServingRadiusInKms);
            });
          }          
        }
}

