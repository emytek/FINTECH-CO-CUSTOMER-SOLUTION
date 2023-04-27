import { calculateDistance, parseCustomerData, findCustomersWithinRadius, writeToMessageBroker } from './helpers'; 
import * as fs from 'fs';


// Customer type
interface Customer {
  id: string;
  lat: number;
  long: number;
}

type Coordinates = {
    lat: number;
    long: number;
};
  
//Read the Customer data and parse it
const customersData = fs.readFileSync('./customers.txt', 'utf-8'); // Replace 'customers.txt' with the path to your customer data file
const customers = parseCustomerData(customersData);

//Set Coordinates of FINTECH CO
const fintechCoCoordinates = {
    lat: 52.493256,
    long: 13.446082,
};

//Calculate the distances and find the customers within the desired radius
const invitedCustomers = findCustomersWithinRadius(customers, fintechCoCoordinates, 100);

if (invitedCustomers.length === 0) {
    console.warn('No customers found within the specified radius.');
  } else {
    // Write the invited customers to the message broker
    writeToMessageBroker(invitedCustomers)
      .then(() => console.log('Invited customers have been sent to the message broker.'))
      .catch((error) => console.error('Failed to write invited customers to the message broker:', error));
  }
  


  
