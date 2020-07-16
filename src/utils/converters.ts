import { Message } from '../models/message';
import { VALIDATION_ERROR } from '../models/consts';;

export function convertMessageDTO(message: any): Message {
    if (!message.text || message.text === '' || typeof message.text !== 'string') {
        let err = new Error('Wrong message format');
        err.name = VALIDATION_ERROR;
        throw err;
    }
    if (!message.timestamp || isNaN(message.timestamp)) {
        let err = new Error('Wrong timestamp format');
        err.name = VALIDATION_ERROR;
        throw err;
    }
    return {
        id: 0,
        text: message.text,
        timestamp: new Date(message.timestamp),
    }
}

export function convertMessagesFromStream(streams: EventStream[]): Message[] {
    const messages = streams.map(st => st[1]
        .reduce((acc, m) => acc.concat([m]), [])
        .map((e) => convertEventToMessage(e)
        ));
    return messages[0];
}

export function convertEventToMessage(event: EventStreamMessage): Message {
    const id = parseInt(event[0]);
    const timestampIndex = event[1].indexOf('timestamp') + 1;
    const timestamp = new Date(parseInt(event[1][timestampIndex]));
    const textIndex = event[1].indexOf('text') + 1;
    const text = event[1][textIndex];
    return {
        id,
        timestamp,
        text
    }
}

export function messageRedisID(message: Message): string {
    return `${message.timestamp.getTime()}:${message.text}`;
}

export function convertMessageFromRedisSet(redisMessage: string): Message {
    const i = redisMessage.indexOf(':');
    const timestamp = parseInt(redisMessage.slice(0, i));
    const text = redisMessage.slice(i + 1);
    return {
        id: 0,
        timestamp: new Date(timestamp),
        text,
    }
}