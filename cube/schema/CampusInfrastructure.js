cube(`CampusInfrastructure`, {
  sql: `
    SELECT 
      facility_id,
      building,
      room,
      occupancy,
      temperature_celsius,
      energy_usage_kwh,
      timestamp
    FROM (
      SELECT 
        json_array_elements(data::json) ->> 'facility_id' as facility_id,
        json_array_elements(data::json) ->> 'building' as building,
        json_array_elements(data::json) ->> 'room' as room,
        CAST(json_array_elements(data::json) ->> 'occupancy' AS INT) as occupancy,
        CAST(json_array_elements(data::json) ->> 'temperature_celsius' AS FLOAT) as temperature_celsius,
        CAST(json_array_elements(data::json) ->> 'energy_usage_kwh' AS FLOAT) as energy_usage_kwh,
        json_array_elements(data::json) ->> 'timestamp' as timestamp
      FROM (
        SELECT data::json FROM (
          VALUES 
            ('[{"facility_id": "1", "building": "Main", "room": "101", "occupancy": 45, "temperature_celsius": 22.5, "energy_usage_kwh": 150, "timestamp": "2026-05-30T10:00:00"}]')
        ) AS t(data)
      ) AS parsed
    ) AS facilities
  `,
  
  measures: {
    avgOccupancy: {
      sql: `occupancy`,
      type: `avg`,
      format: `number`
    },
    
    maxOccupancy: {
      sql: `occupancy`,
      type: `max`
    },
    
    totalEnergyUsage: {
      sql: `energy_usage_kwh`,
      type: `sum`,
      format: `number`
    },
    
    avgTemperature: {
      sql: `temperature_celsius`,
      type: `avg`,
      format: `number`
    },
    
    utilizationRate: {
      sql: `${CUBE}.occupancy / 100`,
      type: `avg`,
      format: `percent`
    }
  },
  
  dimensions: {
    facilityId: {
      sql: `facility_id`,
      type: `string`,
      primaryKey: true
    },
    
    building: {
      sql: `building`,
      type: `string`,
      meta: {
        description: `Campus building name`
      }
    },
    
    room: {
      sql: `room`,
      type: `string`
    },
    
    occupancy: {
      sql: `occupancy`,
      type: `number`
    },
    
    temperatureCelsius: {
      sql: `temperature_celsius`,
      type: `number`
    },
    
    energyUsageKwh: {
      sql: `energy_usage_kwh`,
      type: `number`
    },
    
    timestamp: {
      sql: `timestamp`,
      type: `time`
    },
    
    hourOfDay: {
      sql: `EXTRACT(HOUR FROM CAST(${CUBE}.timestamp AS TIMESTAMP))`,
      type: `number`
    }
  },
  
  segments: {
    highTraffic: {
      sql: `${CUBE}.occupancy > 80`
    },
    
    lowTraffic: {
      sql: `${CUBE}.occupancy < 30`
    }
  }
});