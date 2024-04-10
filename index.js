import amqp from "amqplib";
import { PrismaClient } from "@prisma/client";
import { sendEmail } from "./email.js";
const prisma = new PrismaClient();
const notifyStudent = async (input, channel) => {
  const students = await prisma.student.findMany({
    select: { email: true },
  });

  console.log("processing message");

  students.forEach(async (student) => {
    try {
      await sendEmail({
        to: student.email,
        text: input.text,
        html: input.html,
        subject: input.subject,
      });
    } catch (error) {
      channel.sendToQueue("notifications", Buffer.from(JSON.stringify(input)));
      console.log(error);
    }
  });
};
async function connect() {
  try {
    const amqpServer = "amqp://localhost:5672";
    const connection = await amqp.connect(amqpServer);
    const channel = await connection.createChannel();
    await channel.assertQueue("notifications");
    await channel.consume("notifications", async (message) => {
      const input = JSON.parse(message.content.toString());
      console.log(input);
      const companyId = input.companyId;
      if (!companyId) throw new Error("invalid inputs");
      const company = await prisma.company.findUnique({
        where: { id: Number(companyId) },
      });
      if (!company) throw new Error("invalid inputs");
      notifyStudent(input);
      channel.ack(message);
    });

    console.log("Waiting for messages...");
  } catch (ex) {
    console.error(ex);
  }
}

connect();
