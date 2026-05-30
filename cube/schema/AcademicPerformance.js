cube(`AcademicPerformance`, {
  sql: `
    SELECT 
      record_id,
      user_id as student_id,
      course_id,
      semester,
      grade,
      attendance_rate,
      assignments_completed,
      assignments_total
    FROM (
      SELECT 
        json_array_elements(data::json) ->> 'record_id' as record_id,
        json_array_elements(data::json) ->> 'user_id' as user_id,
        json_array_elements(data::json) ->> 'course_id' as course_id,
        json_array_elements(data::json) ->> 'semester' as semester,
        CAST(json_array_elements(data::json) ->> 'grade' AS FLOAT) as grade,
        CAST(json_array_elements(data::json) ->> 'attendance_rate' AS FLOAT) as attendance_rate,
        CAST(json_array_elements(data::json) ->> 'assignments_completed' AS INT) as assignments_completed,
        CAST(json_array_elements(data::json) ->> 'assignments_total' AS INT) as assignments_total
      FROM (
        SELECT data::json FROM (
          VALUES 
            ('[{"record_id": "1", "user_id": "1001", "course_id": "CS101", "semester": "2026-Spring", "grade": 4.5, "attendance_rate": 0.85, "assignments_completed": 8, "assignments_total": 10}]')
        ) AS t(data)
      ) AS parsed
    ) AS grades
  `,
  
  measures: {
    avgGrade: {
      sql: `grade`,
      type: `avg`,
      format: `number`,
      meta: {
        description: `Average student grade`
      }
    },
    
    avgAttendance: {
      sql: `attendance_rate`,
      type: `avg`,
      format: `percent`
    },
    
    completionRate: {
      sql: `${CUBE}.assignments_completed / NULLIF(${CUBE}.assignments_total, 0)`,
      type: `avg`,
      format: `percent`
    },
    
    totalAssignments: {
      sql: `assignments_total`,
      type: `sum`
    },
    
    studentCount: {
      sql: `student_id`,
      type: `countDistinct`
    },
    
    passingRate: {
      sql: `CASE WHEN ${CUBE}.grade >= 3.0 THEN 1 ELSE 0 END`,
      type: `avg`,
      format: `percent`
    },
    
    gradeDistribution: {
      sql: `CASE 
        WHEN ${CUBE}.grade >= 4.5 THEN 'excellent'
        WHEN ${CUBE}.grade >= 3.5 THEN 'good'
        WHEN ${CUBE}.grade >= 3.0 THEN 'satisfactory'
        ELSE 'needs_improvement'
      END`,
      type: `count`,
      drillMembers: [studentId, courseId]
    }
  },
  
  dimensions: {
    recordId: {
      sql: `record_id`,
      type: `string`,
      primaryKey: true
    },
    
    studentId: {
      sql: `student_id`,
      type: `number`
    },
    
    courseId: {
      sql: `course_id`,
      type: `string`,
      meta: {
        description: `Course identifier`
      }
    },
    
    semester: {
      sql: `semester`,
      type: `string`
    },
    
    grade: {
      sql: `grade`,
      type: `number`
    },
    
    gradeCategory: {
      sql: `CASE 
        WHEN ${CUBE}.grade >= 4.5 THEN 'excellent'
        WHEN ${CUBE}.grade >= 3.5 THEN 'good'
        WHEN ${CUBE}.grade >= 3.0 THEN 'satisfactory'
        ELSE 'needs_improvement'
      END`,
      type: `string`
    }
  },
  
  joins: {
    Students: {
      sql: `${CUBE}.student_id = ${Students}.student_id`,
      relationship: `belongsTo`
    }
  },
  
  segments: {
    activeStudents: {
      sql: `${CUBE}.grade >= 3.0`
    },
    
    atRisk: {
      sql: `${CUBE}.grade < 3.0`
    },
    
    highAchievers: {
      sql: `${CUBE}.grade >= 4.0`
    }
  }
});