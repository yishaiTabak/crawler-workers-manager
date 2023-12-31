const {Consumer} = require('sqs-consumer');
const {SQSClient } = require('@aws-sdk/client-sqs');
const axios = require('axios')

const workers = [
  {
    url: process.env.URL_WORKER_1,
    isBusy:false
  }
]

const taskQueue = [];

const handleMessage = async (message) => {
  const availableWorker = workers.find(w => !w.isBusy);

    if (availableWorker) {
      availableWorker.isBusy = true;
      axios.post(availableWorker.url+"/start", JSON.parse(message.Body), { timeout: 300000 })
        .then(() => {
          availableWorker.isBusy = false;
          checkForMoreMission(availableWorker)
        })
        .catch((error) => {
          console.error('Error during message processing:', error.message);
          availableWorker.isBusy = false;
        });
    } else {
      taskQueue.push(JSON.parse(message.Body));
      console.log("Task enqueued for later processing");
    }
  console.log(taskQueue.length);
}

const listener = Consumer.create({
  queueUrl: process.env.QUEUE_URL,
  handleMessage,
  sqs: new SQSClient({
    region: process.env.AWS_REGION
  })
});

listener.on('error', (err) => {
  console.error(err.message);
});

listener.on('processing_error', (err) => {
  console.error(err.message);
});

listener.on('timeout_error', (err) => {
  console.error(err.message);
});

listener.start();

const checkForMoreMission = async (availableWorker)=>{
  if (taskQueue.length > 0) {
    const nextTask = taskQueue.shift();
    availableWorker.isBusy = true;
    axios.post(availableWorker.url+"/start", nextTask,{ timeout: 300000 })
      .then(() => {
        availableWorker.isBusy = false;
        checkForMoreMission(availableWorker)
      })
    }
}