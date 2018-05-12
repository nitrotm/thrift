/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


/**
 * Thrift TypeScript library version.
 */
export const TVersion = '1.0.0-dev';


/**
 * Helpers
 */
export const isNull = (value: any) => value === undefined || value === null;


/**
 * Thrift IDL type string to Id mapping.
 *
 *   STOP   - End of a set of fields.
 *   VOID   - No value (only legal for return types).
 *   BOOL   - True/False integer.
 *   BYTE   - Signed 8 bit integer.
 *   I08    - Signed 8 bit integer.
 *   DOUBLE - 64 bit IEEE 854 floating point.
 *   I16    - Signed 16 bit integer.
 *   I32    - Signed 32 bit integer.
 *   I64    - Signed 64 bit integer.
 *   STRING - Array of bytes representing a string of characters.
 *   UTF7   - Array of bytes representing a string of UTF7 encoded characters.
 *   STRUCT - A multifield type.
 *   MAP    - A collection type (map/associative-array/dictionary).
 *   SET    - A collection type (unordered and without repeated values).
 *   LIST   - A collection type (unordered).
 *   UTF8   - Array of bytes representing a string of UTF8 encoded characters.
 *   UTF16  - Array of bytes representing a string of UTF16 encoded characters.
 */
export enum TType {
  STOP   = 0,
  VOID   = 1,
  BOOL   = 2,
  BYTE   = 3,
  I08    = 3,
  DOUBLE = 4,
  I16    = 6,
  I32    = 8,
  I64    = 10,
  STRING = 11,
  UTF7   = 11,
  STRUCT = 12,
  MAP    = 13,
  SET    = 14,
  LIST   = 15,
  UTF8   = 16,
  UTF16  = 17,
}

/**
 * Thrift RPC message type string to Id mapping.
 *
 *   CALL      - RPC call sent from client to server.
 *   REPLY     - RPC call normal response from server to client.
 *   EXCEPTION - RPC call exception response from server to client.
 *   ONEWAY    - Oneway RPC call from client to server with no response.
 */
export enum TMessageType {
  CALL      = 1,
  REPLY     = 2,
  EXCEPTION = 3,
  ONEWAY    = 4,
}


/**
 * Thrift Protocol Exception type string to Id mapping.
 *
 *   UNKNOWN - Unknown/undefined.
 */
export enum TTransportExceptionType {
  UNKNOWN = 0,
}

/**
 * Thrift Protocol Exception type string to Id mapping.
 *
 *   UNKNOWN                 - Unknown/undefined.
 *   INVALID_DATA            - ?
 *   NEGATIVE_SIZE           - ?
 *   SIZE_LIMIT              - ?
 *   BAD_VERSION             - ?
 *   NOT_IMPLEMENTED         - ?
 */
export enum TProtocolExceptionType {
  UNKNOWN         = 0,
  INVALID_DATA    = 1,
  NEGATIVE_SIZE   = 2,
  SIZE_LIMIT      = 3,
  BAD_VERSION     = 4,
  NOT_IMPLEMENTED = 5,
}


/**
 * Generic exception class for Thrift.
 *
 */
export class TException extends Error {
  /**
   * Constructor
   */
  constructor(message?: string) {
    super(message);
  }
}


/**
 * Transport exception class for Thrift.
 *
 */
export class TTransportException extends TException {
  /**
   * Constructor
   */
  constructor(readonly code: TTransportExceptionType = TTransportExceptionType.UNKNOWN, message?: string) {
    super(message);
  }
}


/**
 * Generic interface that encapsulates the I/O layer.
 *
 */
export interface TTransport {
  read(): Promise<string>;

  write(data: string): Promise<void>;
}


/**
 * Protocol exception class for Thrift.
 *
 */
export class TProtocolException extends TException {
  /**
   * Constructor
   */
  constructor(readonly code: TProtocolExceptionType = TProtocolExceptionType.UNKNOWN, message?: string) {
    super(message);
  }


  static throwRequired() {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "missing required field");
  }
}


/**
 * Value interface (read/write).
 *
 */
export interface TValue {
  readonly valueType: TType;
  readonly booleanValue: boolean;
  readonly numberValue: number;
  readonly stringValue: string;
  readonly listValue: TList;
  readonly mapValue: TMap;
  readonly setValue: TSet;
  readonly structValue: TStruct;

  asField(): TField;
}


/**
 * Helper class that encapsulates basic metadata and data.
 *
 */
export class TBase implements TValue {
  /**
   * Constructor
   */
  constructor(
    readonly valueType: TType,
    readonly value?: boolean | number | string
  ) {}


  get booleanValue(): boolean {
    switch (this.valueType) {
      case TType.BOOL:
        return this.value as boolean;
      default:
        throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected boolean value");
    }
  }

  get numberValue(): number {
    switch (this.valueType) {
      case TType.BYTE:
      case TType.I08:
      case TType.I16:
      case TType.I32:
      case TType.I64:
      case TType.DOUBLE:
        return this.value as number;
      default:
        throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected number value");
    }
  }

  get stringValue(): string {
    switch (this.valueType) {
      case TType.STRING:
      case TType.UTF7:
      case TType.UTF8:
      case TType.UTF16:
        return this.value as string;
      default:
        throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected string value");
    }
  }

  get listValue(): TList {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected list value");
  }

  get mapValue(): TMap {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected map value");
  }

  get setValue(): TSet {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected set value");
  }

  get structValue(): TStruct {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected struct value");
  }


  asField(): TField {
    return new TField(this.valueType, this);
  }


  static newBool(value: boolean): TBase {
    return new TBase(TType.BOOL, value);
  }

  static newByte(value: number): TBase {
    return new TBase(TType.BYTE, value);
  }

  static newI08(value: number): TBase {
    return new TBase(TType.I08, value);
  }

  static newI16(value: number): TBase {
    return new TBase(TType.I16, value);
  }

  static newI32(value: number): TBase {
    return new TBase(TType.I32, value);
  }

  static newI64(value: number): TBase {
    return new TBase(TType.I64, value);
  }

  static newDouble(value: number): TBase {
    return new TBase(TType.DOUBLE, value);
  }

  static newString(value: string): TBase {
    return new TBase(TType.STRING, value);
  }
}


/**
 * Helper class that encapsulates list metadata and data.
 *
 */
export class TList implements TValue {
  /**
   * Constructor
   */
  constructor(
    readonly itemType: TType,
    readonly items: TValue[]
  ) {}


  get valueType(): TType {
    return TType.LIST;
  }

  get booleanValue(): boolean {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected boolean value");
  }

  get numberValue(): number {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected number value");
  }

  get stringValue(): string {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected string value");
  }

  get listValue(): TList {
    return this;
  }

  get mapValue(): TMap {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected map value");
  }

  get setValue(): TSet {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected set value");
  }

  get structValue(): TStruct {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected struct value");
  }


  asField(): TField {
    return new TField(TType.LIST, this);
  }


  map<T>(f: (value: TValue) => T): T[] {
    let result: T[] = [];

    for (let item of this.items) {
      result.push(f(item));
    }
    return result;
  }


  static newList<T>(itemType: TType, items: T[], f: (value: T) => TValue): TList {
    let result: TValue[] = [];

    for (let item of items) {
      result.push(f(item));
    }
    return new TList(itemType, result);
  }
}


/**
 * Helper class that encapsulates map metadata and data.
 *
 */
export class TMap implements TValue {
  /**
   * Constructor
   */
  constructor(
    readonly keyType: TType,
    readonly itemType: TType,
    readonly items: TMapEntry[]
  ) {}


  get valueType(): TType {
    return TType.MAP;
  }

  get booleanValue(): boolean {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected boolean value");
  }

  get numberValue(): number {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected number value");
  }

  get stringValue(): string {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected string value");
  }

  get listValue(): TList {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected list value");
  }

  get mapValue(): TMap {
    return this;
  }

  get setValue(): TSet {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected set value");
  }

  get structValue(): TStruct {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected struct value");
  }


  asField(): TField {
    return new TField(TType.MAP, this);
  }


  mapGeneric<K, V>(fk: (key: TValue) => K, fv: (value: TValue) => V): any {
    let result: any = {};

    for (let item of this.items) {
      result[fk(item.key)] = fv(item.value);
    }
    return result;
  }

  mapNumber<V>(fk: (key: TValue) => number, fv: (value: TValue) => V): { [k: number]: V } {
    let result: { [k: number]: V } = {};

    for (let item of this.items) {
      result[fk(item.key)] = fv(item.value);
    }
    return result;
  }

  mapString<V>(fk: (key: TValue) => string, fv: (value: TValue) => V): { [k: string]: V } {
    let result: { [k: string]: V } = {};

    for (let item of this.items) {
      result[fk(item.key)] = fv(item.value);
    }
    return result;
  }


  static newMap<K, V>(keyType: TType, itemType: TType, items: any, fk: (key: K) => TValue, fv: (value: V) => TValue): TMap {
    let result: TMapEntry[] = [];

    for (let item in items) {
      result.push(new TMapEntry(fk(<any>item as K), fv(items[item])));
    }
    return new TMap(keyType, itemType, result);
  }
}


/**
 * Helper class that encapsulates map metadata and data.
 *
 */
export class TMapEntry {
  /**
   * Constructor
   */
  constructor(
    readonly key: TValue,
    readonly value: TValue
  ) {}
}


/**
 * Helper class that encapsulates set metadata and data.
 *
 */
export class TSet implements TValue {
  /**
   * Constructor
   */
  constructor(
    readonly itemType: TType,
    readonly items: TValue[]
  ) {}


  get valueType(): TType {
    return TType.SET;
  }

  get booleanValue(): boolean {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected boolean value");
  }

  get numberValue(): number {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected number value");
  }

  get stringValue(): string {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected string value");
  }

  get listValue(): TList {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected list value");
  }

  get mapValue(): TMap {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected map value");
  }

  get setValue(): TSet {
    return this;
  }

  get structValue(): TStruct {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected struct value");
  }


  asField(): TField {
    return new TField(TType.SET, this);
  }


  map<T>(f: (value: TValue) => T): T[] {
    let result: T[] = [];

    for (let item of this.items) {
      result.push(f(item));
    }
    return result;
  }


  static newSet<T>(itemType: TType, items: T[], f: (value: T) => TValue): TSet {
    let result: TValue[] = [];

    for (let item of items) {
      result.push(f(item));
    }
    return new TSet(itemType, result);
  }
}


/**
 * Helper class that encapsulates struct metadata and data.
 *
 */
export class TStruct implements TValue {
  /**
   * Constructor
   */
  constructor(
    readonly fields: { [k: number]: TField }
  ) {}


  get valueType(): TType {
    return TType.STRUCT;
  }

  get booleanValue(): boolean {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected boolean value");
  }

  get numberValue(): number {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected number value");
  }

  get stringValue(): string {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected string value");
  }

  get listValue(): TList {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected list value");
  }

  get mapValue(): TMap {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected map value");
  }

  get setValue(): TSet {
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "expected set value");
  }

  get structValue(): TStruct {
    return this;
  }


  asField(): TField {
    return new TField(TType.STRUCT, this);
  }


  getField(id: number, fieldType: TType): TField {
    if (this.fields[id] && this.fields[id].fieldType === fieldType) {
      return this.fields[id];
    }
    return null;
  }
}


/**
 * Helper class that encapsulates field metadata.
 *
 */
export class TField {
  /**
   * Constructor
   */
  constructor(
    readonly fieldType: TType,
    readonly value: TValue
  ) {}


  get booleanValue(): boolean {
    return this.value.booleanValue;
  }

  get numberValue(): number {
    return this.value.numberValue;
  }

  get stringValue(): string {
    return this.value.stringValue;
  }

  get listValue(): TList {
    return this.value.listValue;
  }

  get mapValue(): TMap {
    return this.value.mapValue;
  }

  get setValue(): TSet {
    return this.value.setValue;
  }

  get structValue(): TStruct {
    return this.value.structValue;
  }
}


/**
 * Helper class that encapsulates a struct metadata and data.
 *
 */
export class TMessage {
  /**
   * Constructor
   */
  constructor(
    readonly name: string,
    readonly messageType: TMessageType,
    readonly seqid: number,
    readonly value: TValue
  ) {}


  get booleanValue(): boolean {
    return this.value.booleanValue;
  }

  get numberValue(): number {
    return this.value.numberValue;
  }

  get stringValue(): string {
    return this.value.stringValue;
  }

  get listValue(): TList {
    return this.value.listValue;
  }

  get mapValue(): TMap {
    return this.value.mapValue;
  }

  get setValue(): TSet {
    return this.value.setValue;
  }

  get structValue(): TStruct {
    return this.value.structValue;
  }
}


/**
 * Protocol interface definition.
 *
 */
export interface TProtocol {
  /**
   * Underlying transport
   */

  readonly transport: TTransport;


  /**
   * Writing methods.
   */

  writeMessage(message: TMessage): Promise<void>;

  writeStruct(struct: TStruct): Promise<void>;

  /**
   * Reading methods.
   */

  readMessage(): Promise<TMessage>;

  readStruct(): Promise<TStruct>;
}


/**
 * JSON protocol implementation for thrift.
 *
 * This is a full-featured protocol supporting write and read.
 *
 */
export class TJSONProtocol implements TProtocol {
  public static readonly VERSION: number = 1;


  private static typeString(type: TType): string {
    switch (type) {
      case TType.BOOL:
        return 'tf';
      case TType.I08:
        return 'i8';
      case TType.I16:
        return 'i16';
      case TType.I32:
        return 'i32';
      case TType.I64:
        return 'i64';
      case TType.DOUBLE:
        return 'dbl';
      case TType.STRING:
        return 'str';
      case TType.LIST:
        return 'lst';
      case TType.MAP:
        return 'map';
      case TType.SET:
        return 'set';
      case TType.STRUCT:
        return 'rec';
    }
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "unsupported type (" + type + ")")
  }

  private static typeOfString(type: string): TType {
    switch (type) {
      case 'tf':
        return TType.BOOL;
      case 'i8':
        return TType.I08;
      case 'i16':
        return TType.I16;
      case 'i32':
        return TType.I32;
      case 'i64':
        return TType.I64;
      case 'str':
        return TType.STRING;
      case 'dbl':
        return TType.DOUBLE;
      case 'lst':
        return TType.LIST;
      case 'map':
        return TType.MAP;
      case 'set':
        return TType.SET;
      case 'rec':
        return TType.STRUCT;
    }
    throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "unsupported type (" + type + ")")
  }


  /**
   * Constructor
   */
  constructor(readonly transport: TTransport) {
  }


  writeMessage(message: TMessage): Promise<void> {
    let data = [
      TJSONProtocol.VERSION,
      message.name,
      message.messageType,
      message.seqid,
      this.serializeValue(message.value)
    ];

    return this.transport.write(JSON.stringify(data));
  }

  writeStruct(struct: TStruct): Promise<void> {
    let data = {};

    for (let id in struct.fields) {
      let field = struct.fields[id];
      let item = {};

      item[TJSONProtocol.typeString(field.fieldType)] = this.serializeValue(field.value);
      data[id] = item;
    }

    return this.transport.write(JSON.stringify(data));
  }


  readMessage(): Promise<TMessage> {
    return this.transport.read().then( text => {
      let data = JSON.parse(text);

      // check message format
      if (!data || data.length != 5) {
        throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "illegal data format");
      }

      // decode message frame
      let version: number = data[0];
      let name: string = data[1];
      let messageType: TMessageType = data[2];
      let seqid: number = data[3];
      let value: any = data[4];

      // check version
      if (version != TJSONProtocol.VERSION) {
        throw new TProtocolException(TProtocolExceptionType.BAD_VERSION, "unsupported version (" + version + ")");
      }
      return new TMessage(name, messageType, seqid, this.deserializeValue(TType.STRUCT, value));
    });
  }

  readStruct(): Promise<TStruct> {
    return this.transport.read().then( text => {
      let data = JSON.parse(text);

      // check message format
      if (!data) {
        throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "illegal data format");
      }
      return this.deserializeValue(TType.STRUCT, data) as TStruct;
    });
  }


  private serializeValue(value: TValue): any {
    if (value === undefined || value === null) {
      return null;
    }
    switch (value.valueType) {
      case TType.BOOL: {
        let base = value as TBase;

        if (base.value === undefined || base.value === null) {
          return null;
        } else if (typeof(base.value) === 'boolean') {
          let data = <boolean>base.value;

          return data ? 1 : 0;
        }
        throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "boolean expected (" + base.value + ")");
      }
      case TType.BYTE:
      case TType.I08:
      case TType.I16:
      case TType.I32:
      case TType.I64:
      case TType.DOUBLE: {
        let base = value as TBase;

        if (base.value === undefined || base.value === null) {
          return null;
        } else if (typeof(base.value) === 'number') {
          let data = <number>base.value;

          if (isNaN(data)) {
            throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "number is NaN");
          }
          return data;
        }
        throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "number expected (" + base.value + ")");
      }
      case TType.UTF7:
      case TType.UTF8:
      case TType.UTF16:
      case TType.STRING: {
        let base = value as TBase;

        if (base.value === undefined || base.value === null) {
          return null;
        } else if (typeof(base.value) === 'string') {
          let data = <string>base.value;

          return data;
        }
        throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "string expected (" + base.value + ")");
      }
      case TType.LIST: {
        let list = value as TList;
        let data = [];

        data.push(TJSONProtocol.typeString(list.itemType));
        data.push(list.items.length);
        for (let item of list.items) {
          data.push(this.serializeValue(item));
        }
        return data;
      }
      case TType.MAP: {
        let map = value as TMap;
        let data2 = {};

        for (let item of map.items) {
          data2[this.serializeValue(item.key)] = this.serializeValue(item.value);
        }

        let data = [];

        data.push(TJSONProtocol.typeString(map.keyType));
        data.push(TJSONProtocol.typeString(map.itemType));
        data.push(map.items.length);
        data.push(data2);
        return data;
      }
      case TType.SET: {
        let set = value as TSet;
        let data = [];

        data.push(TJSONProtocol.typeString(set.itemType));
        data.push(set.items.length);
        for (let item of set.items) {
          data.push(this.serializeValue(item));
        }
        return data;
      }
      case TType.STRUCT: {
        let struct = value as TStruct;
        let data = {};

        for (let id in struct.fields) {
          let field = struct.fields[id];
          let item = {};

          item[TJSONProtocol.typeString(field.fieldType)] = this.serializeValue(field.value);
          data[id] = item;
        }
        return data;
      }
      default:
        throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "unsupported value type (" + value + ")");
    }
  }

  private deserializeValue(valueType: TType, value: any): TValue {
    switch (valueType) {
      case TType.BOOL:
        if (value === undefined || value === null) {
          return new TBase(valueType);
        }
        return new TBase(valueType, value ? true : false);
      case TType.BYTE:
      case TType.I08:
      case TType.I16:
      case TType.I32:
      case TType.I64:
      case TType.DOUBLE:
        if (value === undefined || value === null) {
          return new TBase(valueType);
        }
        if (typeof(value) === 'number') {
          return new TBase(valueType, value);
        }
        throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "number expected: " + JSON.stringify(value));
      case TType.UTF7:
      case TType.UTF8:
      case TType.UTF16:
      case TType.STRING:
        if (value === undefined || value === null) {
          return new TBase(valueType);
        }
        if (typeof(value) === 'string') {
          return new TBase(valueType, value);
        }
        throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "string expected: " + JSON.stringify(value));
      case TType.LIST: {
        let list = value as any[];

        if (!list || list.length < 2) {
          throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "list expected: " + JSON.stringify(value));
        }

        let itemType = TJSONProtocol.typeOfString(list.shift());
        let size = list.shift();
        let data = list;
        let items: TValue[] = [];

        for (let item of data) {
          items.push(this.deserializeValue(itemType, item));
        }
        if (items.length != size) {
          throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "list size mismatch: " + items.length + " != " + size);
        }
        return new TList(itemType, items);
      }
      case TType.MAP: {
        let map = value as any[];

        if (!map || map.length != 4) {
          throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "map expected: " + JSON.stringify(value));
        }

        let keyType = TJSONProtocol.typeOfString(map.shift());
        let itemType = TJSONProtocol.typeOfString(map.shift());
        let size = map.shift();
        let data = map.shift();
        let items: TMapEntry[] = [];

        for (let key in data) {
          items.push(
            new TMapEntry(
              this.deserializeValue(keyType, key),
              this.deserializeValue(itemType, data[key])
            )
          );
        }
        if (items.length != size) {
          throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "map size mismatch: " + items.length + " != " + size);
        }
        return new TMap(keyType, itemType, items);
      }
      case TType.SET: {
        let set = value as any[];

        if (!set || set.length < 2) {
          throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "set expected: " + JSON.stringify(value));
        }

        let itemType = TJSONProtocol.typeOfString(set.shift());
        let size = set.shift();
        let data = set;
        let items: TValue[] = [];

        for (let item of data) {
          items.push(this.deserializeValue(itemType, item));
        }
        if (items.length != size) {
          throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "set size mismatch: " + items.length + " != " + size);
        }
        return new TSet(itemType, items);
      }
      case TType.STRUCT: {
        let struct = value as { [k: number]: { [k2: string]: any } };

        if (!struct) {
          throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "struct expected: " + JSON.stringify(value));
        }

        let fields: { [k: number]: TField } = {};

        for (let id in struct) {
          for (let type in struct[id]) {
            let fieldType = TJSONProtocol.typeOfString(type);

            fields[parseInt(id)] = new TField(
              fieldType,
              this.deserializeValue(fieldType, struct[id][type])
            );
          }
        }
        return new TStruct(fields);
      }
      default:
        throw new TProtocolException(TProtocolExceptionType.INVALID_DATA, "unsupported value type (" + value + ")");
    }
  }
}


/**
 * Thrift Application Exception type string to Id mapping.
 *
 *   UNKNOWN                 - Unknown/undefined.
 *   UNKNOWN_METHOD          - Client attempted to call a method unknown to the server.
 *   INVALID_MESSAGE_TYPE    - Client passed an unknown/unsupported MessageType.
 *   WRONG_METHOD_NAME       - Unused.
 *   BAD_SEQUENCE_ID         - Unused in Thrift RPC, used to flag proprietary sequence number errors.
 *   MISSING_RESULT          - Raised by a server processor if a handler fails to supply the required return result.
 *   INTERNAL_ERROR          - Something bad happened.
 *   PROTOCOL_ERROR          - The protocol layer failed to serialize or deserialize data.
 *   INVALID_TRANSFORM       - Unused.
 *   INVALID_PROTOCOL        - The protocol (or version) is not supported.
 *   UNSUPPORTED_CLIENT_TYPE - Unused.
 */
export enum TApplicationExceptionType {
  UNKNOWN                 = 0,
  UNKNOWN_METHOD          = 1,
  INVALID_MESSAGE_TYPE    = 2,
  WRONG_METHOD_NAME       = 3,
  BAD_SEQUENCE_ID         = 4,
  MISSING_RESULT          = 5,
  INTERNAL_ERROR          = 6,
  PROTOCOL_ERROR          = 7,
  INVALID_TRANSFORM       = 8,
  INVALID_PROTOCOL        = 9,
  UNSUPPORTED_CLIENT_TYPE = 10,
}


/**
 * Generic exception class for Thrift.
 *
 */
export class TApplicationException extends TException {
  /**
   * Constructor
   */
  constructor(public code: TApplicationExceptionType = TApplicationExceptionType.UNKNOWN, message?: string) {
    super(message);
  }


  read(input: TProtocol, data: TStruct): TApplicationException {
    let field: TField;
    field = data.getField(1, TType.I32);
    if (field) {
      this.code = field.numberValue as TApplicationExceptionType;
    } else {
      delete this.code;
    }
    field = data.getField(2, TType.STRING);
    if (field) {
      this.message = field.stringValue;
    } else {
      delete this.message;
    }
    return this;
  }

  write(output: TProtocol): TStruct {
    let fields: { [k: number]: TField } = {};
    if (!isNull(this.code)) {
      fields[1] = TBase.newI32(this.code as number).asField();
    }
    if (!isNull(this.message)) {
      fields[2] = TBase.newString(this.message).asField();
    }
    return new TStruct(fields);
  }
}
