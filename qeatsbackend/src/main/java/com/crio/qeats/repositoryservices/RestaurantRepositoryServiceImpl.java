/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.repositoryservices;

import ch.hsr.geohash.GeoHash;
import com.crio.qeats.configs.RedisConfiguration;
import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.globals.GlobalConstants;
import com.crio.qeats.models.RestaurantEntity;
import com.crio.qeats.models.ItemEntity;
import com.crio.qeats.models.MenuEntity;
import com.crio.qeats.models.RestaurantEntity;
import com.crio.qeats.repositories.ItemRepository;
import com.crio.qeats.repositories.MenuRepository;
import com.crio.qeats.repositories.RestaurantRepository;
import com.crio.qeats.utils.GeoLocation;
import com.crio.qeats.utils.GeoUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;


@Service
public class RestaurantRepositoryServiceImpl implements RestaurantRepositoryService {


  
  private Restaurant restaurant;
  
  @Autowired
  private RestaurantRepository restaurantRepository;

  @Autowired
  private RedisConfiguration redisConfiguration;

  @Autowired
  private MongoTemplate mongoTemplate;

  @Autowired
  private Provider<ModelMapper> modelMapperProvider;


  @Autowired
  ItemRepository itemRepository;

  @Autowired
  MenuRepository menuRepository;

  private boolean isOpenNow(LocalTime time, RestaurantEntity res) {
    LocalTime openingTime = LocalTime.parse(res.getOpensAt());
    LocalTime closingTime = LocalTime.parse(res.getClosesAt());

    return time.isAfter(openingTime) && time.isBefore(closingTime);
  }

  // TODO: CRIO_TASK_MODULE_NOSQL
  // Objectives:
  // 1. Implement findAllRestaurantsCloseby.
  // 2. Remember to keep the precision of GeoHash in mind while using it as a key.
  // Check RestaurantRepositoryService.java file for the interface contract.
  public List<Restaurant> findAllRestaurantsCloseBy(Double latitude,
      Double longitude, LocalTime currentTime, Double servingRadiusInKms) {
     
        ObjectMapper objectMapper = new ObjectMapper();
        String key = GeoHash.withCharacterPrecision(latitude, longitude, 7).toBase32();
    // List<Restaurant> restaurants = null;



    
    ModelMapper modelMapper = modelMapperProvider.get();
    List<Restaurant> restaurants = new ArrayList<Restaurant>();

    Jedis jedis = redisConfiguration.getJedisPool().getResource();


    if (redisConfiguration.isCacheAvailable() && jedis.get(key) != null) {
      try {
        restaurants = objectMapper.readValue(jedis.get(key),
        new TypeReference<ArrayList<Restaurant>>(){});
	    } catch (IOException e) {
		   // TODO Auto-generated catch block
		    e.printStackTrace();
      } 
      return restaurants;
    }
    long startTime = System.currentTimeMillis();
    List<RestaurantEntity> allRestaurants = restaurantRepository.findAll();
    for (RestaurantEntity restaurant : allRestaurants) {
      if (isRestaurantCloseByAndOpen(restaurant, currentTime, 
          latitude, longitude, servingRadiusInKms)) {
        restaurants.add(modelMapper.map(restaurant, Restaurant.class));
      }
    }
     


    
    try {
      String jedRes = objectMapper.writeValueAsString(restaurants);
      jedis.set(key, jedRes);
    } catch (JsonProcessingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    System.out.print(restaurants);
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken in repo== " + (endTime - startTime));

    // List<Restaurant> sList=new ArrayList<Restaurant>();
  
    
    

    // List<Restaurant> s=new ArrayList<Restaurant>();
    // s.add("1","10","AndhraSpice","HsrLayout","https://images.pexels.com/photo",20.027,30.0,
    //  sList);
     
    // restaurant=new Restaurant("10", "10", "name", "city", 
    // "imageUrl", 20.0, 30.0, "18" , "22", sList);

    // s.add(restaurant);

    // List<Restaurant> r=new ArrayList<Restaurant>();
    // r.add("1","10","AndhraSpice","HsrLayout","https://images.pexels.com/photo",20.027,30.0,
    // s);

      //CHECKSTYLE:OFF
      //CHECKSTYLE:ON


    // sList=restaurants.subList(0, 10);
      // System.out.print("size");

      // System.out.print(sList.size());
    return restaurants;
  }







  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants whose names have an exact or partial match with the search query.
  @Override
  public List<Restaurant> findRestaurantsByName(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
        ModelMapper modelMapper = modelMapperProvider.get();

        List<RestaurantEntity> restaurantEntities = findRestaurantsByNameExact(searchString);
        List<Restaurant> restaurants = new ArrayList<Restaurant>();
         
        if (restaurantEntities == null || restaurantEntities.size() == 0) {
          return restaurants;
        }

        
    for (RestaurantEntity res : restaurantEntities) {
      if (isRestaurantCloseByAndOpen(res, currentTime,latitude, longitude,servingRadiusInKms)) {
        restaurants.add(modelMapper.map(res, Restaurant.class));
      }
    }

    List<RestaurantEntity> restaurantEntities2 = restaurantRepository
    .findRestaurantByName(searchString);

if (restaurantEntities2 != null || restaurantEntities2.size() == 0) {
  return restaurants;
}

for (RestaurantEntity res : restaurantEntities2) {
  if (isRestaurantCloseByAndOpen(res, currentTime,latitude, longitude,servingRadiusInKms)) {
    restaurants.add(modelMapper.map(res, Restaurant.class));
  }
}

 return restaurants;  }


  private List<RestaurantEntity> findRestaurantsByNameExact(String searchString) {


 Optional<List<RestaurantEntity>> restaurantEntities = restaurantRepository
        .findRestaurantsByNameExact(searchString);

    List<RestaurantEntity>  entity = restaurantEntities.get();
    System.out.println(" entity"+restaurantEntities.get());
    
    if (entity == null || entity.size() == 0) {
      return new ArrayList<RestaurantEntity>();
    }   

    return entity;  }

  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants whose attributes (cuisines) intersect with the search query.
  @Override
  public List<Restaurant> findRestaurantsByAttributes(
      Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
        ModelMapper modelMapper = modelMapperProvider.get();

        List<RestaurantEntity> restaurantEntities = restaurantRepository
        .findRestaurantsByAttributesAttributeIn(searchString);
    List<Restaurant> restaurants = new ArrayList<Restaurant>();
    
    if (restaurantEntities == null || restaurantEntities.size() == 0) {
      return restaurants;
    }

    for (RestaurantEntity res : restaurantEntities) {
      if (isRestaurantCloseByAndOpen(res, currentTime,latitude, longitude,servingRadiusInKms)
          && res.getAttributes().contains(searchString)) {
        restaurants.add(modelMapper.map(res, Restaurant.class));
      }
    }

    return restaurants;  }



  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants which serve food items whose names form a complete or partial match
  // with the search query.

  @Override
  public List<Restaurant> findRestaurantsByItemName(
      Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {

        ModelMapper modelMapper = modelMapperProvider.get();

     List<Restaurant> restaurants = new ArrayList<Restaurant>(); 
    List<ItemEntity> itemEntities = itemRepository
        .findItemByname(searchString);
    
    if (itemEntities == null || itemEntities.size() == 0) {
      return restaurants;
    } 

    List<String> itemIdList = new ArrayList<String>();

    Query q = new Query();

    for (ItemEntity ie : itemEntities) {
      itemIdList.add(ie.getId());
    }

    q.addCriteria(Criteria.where("id").in(itemIdList));
        
    List<MenuEntity> menus =  mongoTemplate.find(q, MenuEntity.class);
    ArrayList<String> al = new ArrayList<String>();
    for (MenuEntity m: menus){
      al.add(m.getRestaurantId());
    }
    Query q2 = new Query();
    q2.addCriteria(Criteria.where("restaurantId").in(al));

    List<RestaurantEntity> res_en = mongoTemplate.find(q2, RestaurantEntity.class);

    for (RestaurantEntity res : res_en) {

      if (isRestaurantCloseByAndOpen(res, currentTime,latitude, longitude,servingRadiusInKms)) {
        restaurants.add(modelMapper.map(res, Restaurant.class));
      }
    }

    return restaurants;  
  }

  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants which serve food items whose attributes intersect with the search query.
  @Override
  public List<Restaurant> findRestaurantsByItemAttributes(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
        ModelMapper modelMapper = modelMapperProvider.get();

        List<Restaurant> restaurants = new ArrayList<Restaurant>(); 

        Query q = new Query();
    
        q.addCriteria(Criteria.where("attributes").in(searchString));
    
        List<ItemEntity> itemEntities = mongoTemplate.find(q, ItemEntity.class);
            
        if (itemEntities == null || itemEntities.size() == 0) {
          return restaurants;
        } 
    
        List<String> itemIdList = new ArrayList<String>();
    
        for(ItemEntity ie : itemEntities) {
          itemIdList.add(ie.getId());
        }
    
        q = new Query();
    
        q.addCriteria(Criteria.where("id").in(itemIdList));
            
        List<MenuEntity> menus =  mongoTemplate.find(q, MenuEntity.class);
    
        ArrayList<String> al = new ArrayList<String>();
    
        for(MenuEntity m: menus){
          al.add(m.getRestaurantId());
        }
    
        Query q2 = new Query();
    
        q2.addCriteria(Criteria.where("restaurantId").in(al));
    
        List<RestaurantEntity> res_en = mongoTemplate.find(q2, RestaurantEntity.class);
    
        for(RestaurantEntity res : res_en) {
    
          if(isRestaurantCloseByAndOpen(res, currentTime,latitude, longitude,servingRadiusInKms)) {
             restaurants.add(modelMapper.map(res, Restaurant.class));
          }
        }
    
        return restaurants;    }





  // TODO: CRIO_TASK_MODULE_NOSQL
  // Objective:
  // 1. Check if a restaurant is nearby and open. If so, it is a candidate to be returned.
  // NOTE: How far exactly is "nearby"?

  /**
   * Utility method to check if a restaurant is within the serving radius at a given time.
   * @return boolean True if restaurant falls within serving radius and is open, false otherwise
   */
  private boolean isRestaurantCloseByAndOpen(RestaurantEntity restaurantEntity,
      LocalTime currentTime, Double latitude, Double longitude, Double servingRadiusInKms) {
    if (isOpenNow(currentTime, restaurantEntity)) {
      return GeoUtils.findDistanceInKm(latitude, longitude,
          restaurantEntity.getLatitude(), restaurantEntity.getLongitude())
          < servingRadiusInKms;
    }

    return false;
  }



}

