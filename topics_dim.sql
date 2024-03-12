-- Create a sequence to generate automatic IDs
CREATE SEQUENCE topic_id_seq START 1;

-- Insert data into dim_topic with automatic ID generation
INSERT INTO dim_topic (topic_id, topic)
VALUES 
    (DEFAULT, 'Machine Learning'),
    (DEFAULT, 'Cyber Security'),
    (DEFAULT, 'Python'),
    (DEFAULT, 'Data Science'),
    (DEFAULT, 'Data Engineering'),
    (DEFAULT, 'Data Analytics'),
    (DEFAULT, 'Java Programming'),
    (DEFAULT, 'Big Data'),
    (DEFAULT, 'Blockchain'),
    (DEFAULT, 'AWS Cloud'),
    (DEFAULT, 'Azure Cloud'),
    (DEFAULT, 'Google Cloud Platform'),
    (DEFAULT, 'Generative AI'),
    (DEFAULT, 'DevOps'),
    (DEFAULT, 'Web Development'),
    (DEFAULT, 'Mobile App Development'),
    (DEFAULT, 'Data Structures'),
    (DEFAULT, 'Algorithms');

-- View the populated dim_topic table
SELECT * FROM dim_topic;
