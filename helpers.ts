import * as fs from 'fs';

import amqp from 'amqplib';


interface Customer {
  id: string;
  lat: number;
  long: number;
}

type Coordinates = {
  lat: number;
  long: number;
};

const FINTECH_CO_LAT = 52.493256;
const FINTECH_CO_LONG = 13.446082;
const MAX_DISTANCE = 100; // in kilometers

export function calculateDistance(lat1: number, long1: number, lat2: number, long2: number): number {
  const earthRadius = 6371; // in kilometers
  const latDiff = (lat2 - lat1) * (Math.PI / 180);
  const longDiff = (long2 - long1) * (Math.PI / 180);
  const a =
    Math.sin(latDiff / 2) ** 2 +
    Math.cos(lat1 * (Math.PI / 180)) *
      Math.cos(lat2 * (Math.PI / 180)) *
      Math.sin(longDiff / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  const distance = earthRadius * c;
  return distance;
}

export function parseCustomerData(data: string): Customer[] {
    const lines = data.split('\n');
    const customers: Customer[] = [];
  
    for (const line of lines) {
      const parts = line.split(',');
      if (parts.length === 3) {
        const idMatch = /id: (.+),/.exec(parts[0]);
        const latMatch = /lat: ([\d.]+),/.exec(parts[1]);
        const longMatch = /long: ([\d.]+)/.exec(parts[2]);
  
        if (idMatch && latMatch && longMatch) {
          const id = idMatch[1].trim();
          const lat = parseFloat(latMatch[1].trim());
          const long = parseFloat(longMatch[1].trim());
  
          if (!isNaN(lat) && !isNaN(long)) {
            customers.push({ id, lat, long });
          } else {
            console.warn(`Skipping invalid coordinates for customer ID: ${id}`);
          }
        } else {
          console.warn('Skipping invalid data for a customer');
        }
      }
    }
  
    return customers;
}


export function findCustomersWithinRadius(customers: Customer[], centerCoordinates: Coordinates, radius: number): Customer[] {
  const invitedCustomers: Customer[] = [];  // Update the type to 'Customer[]'
  for (const customer of customers) {
    const distance = calculateDistance(
      FINTECH_CO_LAT,
      FINTECH_CO_LONG,
      customer.lat,
      customer.long
    );
    if (distance <= MAX_DISTANCE) {
      invitedCustomers.push(customer);
    }
  }
  return invitedCustomers.sort();
}


export async function writeToMessageBroker(customers: Customer[]): Promise<void> {
    try {
      const connection = await amqp.connect('amqp://localhost'); // Replace with your RabbitMQ connection URL
      const channel = await connection.createChannel();
      const queueName = 'invitedCustomers';
  
      await channel.assertQueue(queueName, { durable: false });
  
      const customerIds = customers.map((customer) => customer.id); // Extract customer IDs
  
      for (const customerId of customerIds) {
        const message = JSON.stringify(customerId);
        channel.sendToQueue(queueName, Buffer.from(message));
      }
  
      console.log(`Sent ${customers.length} customers to the message broker`);
  
      await channel.close();
      await connection.close();
    } catch (error) {
      console.error('Error occurred while writing to the message broker:', error);
    }
  }
  

export async function consumeFromMessageBroker(): Promise<void> {
    try {
      const connection = await amqp.connect('amqp://localhost'); // Replace with your RabbitMQ connection URL
      const channel = await connection.createChannel();
      const queueName = 'invitedCustomers';
  
      await channel.assertQueue(queueName, { durable: false });
  
      console.log('Waiting for messages...');
  
      channel.consume(queueName, (msg) => {
        if (msg) {
          console.log(msg.content.toString());
          channel.ack(msg);
        }
      });
    } catch (error) {
      console.error('Error occurred while consuming from the message broker:', error);
    }
  }
  
  consumeFromMessageBroker().catch((error) => {
    console.error('An error occurred:', error);
});

async function main(): Promise<void> {
    try {
      const data = fs.readFileSync('customers.txt', 'utf-8');
      const customers = parseCustomerData(data);
      const invitedCustomers = customers.filter((customer) => {
        const distance = calculateDistance(
          52.493256,
          13.446082,
          customer.lat,
          customer.long
        );
        return distance <= 100;
      });
  
      await writeToMessageBroker(invitedCustomers);
      await consumeFromMessageBroker();
    } catch (error) {
      console.error('An error occurred:', error);
    }
  }
  
  
  
  
  
  
  
  
  