cube(`Students`, {
  sql: `
    SELECT 
      student_id,
      student_id as id,
      name,
      email,
      status,
      enrollment_date
    FROM (
      SELECT 
        json_array_elements(data::json) ->> 'student_id' as student_id,
        json_array_elements(data::json) ->> 'name' as name,
        json_array_elements(data::json) ->> 'email' as email,
        json_array_elements(data::json) ->> 'status' as status,
        json_array_elements(data::json) ->> 'enrollment_date' as enrollment_date
      FROM (
        SELECT data::json FROM (
          VALUES 
            ('[{"student_id": "1001", "name": "Student_0", "email": "student_0@university.edu", "status": "active", "enrollment_date": "2026-01-15"}]')
        ) AS t(data)
      ) AS parsed
    ) AS students
  `,
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [studentId, name, email]
    },
    
    activeCount: {
      type: `count`,
      sql: `${CUBE}.status = 'active'`,
      filters: [{ sql: `${CUBE}.status = 'active'` }]
    }
  },
  
  dimensions: {
    studentId: {
      sql: `student_id`,
      type: `number`,
      primaryKey: true,
      shown: true
    },
    
    name: {
      sql: `name`,
      type: `string`
    },
    
    email: {
      sql: `email`,
      type: `string`
    },
    
    status: {
      sql: `status`,
      type: `string`
    },
    
    enrollmentDate: {
      sql: `enrollment_date`,
      type: `time`
    }
  }
});