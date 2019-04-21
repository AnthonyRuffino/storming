/* jshint node:true */ /* global define, escape, unescape */
"use strict";

class Storming {
	constructor({ ip, user, password, database, yourSql, entities, loadDefaultData }) {
		this.orm = require('orm');
		this.password = password;
		this.ip = ip;
		this.user = user;
		this.database = database;
		this.yourSql = yourSql;
		this.entities = entities;
		this.map = {};
		this.loadDefaultData = loadDefaultData === undefined
			|| loadDefaultData === null
			|| loadDefaultData === true
			|| (loadDefaultData.toLowerCase && loadDefaultData.toLocaleLowerCase() === 'true');
			
	}

	getMap() {
		return this.map;
	}

	sync(callback) {

		let entities = this.entities;
		let database = this.database;
		let yourSql = this.yourSql;
		let ip = this.ip;
		let user = this.user;
		let map = this.map;

		let exists = (thing) => {
			return thing !== undefined && thing !== null;
		};

		let isListy = (list) => {
			return exists(list) && list.length > 0;
		};
		let iterate = (list, action) => {
			if (isListy(list)) {
				list.forEach(function(item) {
					action(item);
				});
			}
		};
		let iterateKeys = (obj, action) => {
			if (exists(obj)) {
				let keys = Object.keys(obj);
				iterate(keys, action);
			}
		};
		
		let capFirstLetter = (word) => {
			return word.charAt(0).toUpperCase() + word.slice(1);
		};


		this.orm.connect("mysql://" + user + ":" + this.password + "@" + ip + "/" + database, (err, db) => {
			if (err) throw err;
			

			let hasManyMap = {};
			let hasOneMap = {};
			entities.forEach(function(entity) {
				console.log('Defining table: ' + database + "." + entity.name);
				let model = db.define(entity.name, entity.definition, entity.helpers);

				iterate(entity.hasOne, (owner) => {
					if (exists(map[owner.name]) && exists(map[owner.name].model)) {
						hasOneMap[owner.altName || owner.name] = map[owner.name].model;
						model.hasOne(owner.altName || owner.name, map[owner.name].model, owner.options);
					}
					else {
						console.log('Database owner not found: ' + owner.name);
					}
				});


				iterate(entity.hasMany, (other) => {
					if (exists(map[other.name]) && exists(map[other.name].model)) {
						hasManyMap[other.name] = other;
						model.hasMany(other.desc, map[other.name].model, other.meta || {}, other.options);
					}
				});
				
				const extensions = {};
				iterate(entity.extendsTo, (extension) => {
					console.log(`Defining table: ${entity.name}_${extension.name}`);
					extensions[extension.name] = model.extendsTo(extension.name, extension.data);
				});

				map[entity.name] = { entity: entity, model: model, extensions: extensions };
			});
			
			db.sync((err) => {
				if (err) {
					console.log('Sync err: ' + err);
					callback(err);
					return;
				}
				
				if(this.loadDefaultData) {
					console.log('Loading default data...');
				} else {
					console.log('Skipping default data loading...')
					if(callback) {
						console.log('finishing sync')
						callback();
					}
					return;
				}

				const processDatum = (entity, defaultDatum) => {

					const values = defaultDatum.values;
					map[entity.name].model.find({
						id: values.id
					}, function(err, rows) {
						if (err) throw err;

						if (rows.length > 0) {

							let different = false;

							let keys = Object.keys(values);
							for (let i = 0; i < keys.length; i++) {
								if (rows[0][keys[i]] === undefined) {
									different = true;
								}
								else {
									different = rows[0][keys[i]] !== values[keys[i]];
								}

								if (different)
									break;
							}

							if (different) {
								Object.assign(rows[0], values);
								rows[0].save(function(err) {
									if (err) throw err;
								});
							}



						}
						else {
							console.log('Creating [' + entity.name + ']: ' + values.id);
							let hasMany = defaultDatum.hasMany;
							let extendsTo = defaultDatum.extendsTo;
							let createEntity = (modelValues) => {
								map[entity.name].model.create(modelValues, function(err, createdEntity) {
									if (err) {
										console.log('Error: ' + err);
										throw err;
									}
									iterateKeys(hasMany, (hasManyKey) => {
										hasMany[hasManyKey].forEach(hasManyValue => {
											map[hasManyKey].model.find({
												id: hasManyValue.id
											}, function(err, rows) {
												if (err) throw err;

												if (rows.length > 0) {
													const accessor = hasManyMap[hasManyKey].options.accessor;
													const meta = hasManyMap[hasManyKey].meta && hasManyValue.meta ? hasManyValue.meta : {};
													const addMethodName = 'add' + (accessor ? accessor : capFirstLetter(hasManyKey));
													console.log(entity.name + '[' + createdEntity.id + '].' + addMethodName + '(' + rows[0].id + ')');

													createdEntity[addMethodName](rows[0], meta, function(err) {
														if (err) {
															console.log('Error adding role: ' + err);
														}
													});
												}
											});
										});
									});
									
									
									iterateKeys(extendsTo, (extendsToKey) => {
										extendsTo[extendsToKey][entity.name] = createdEntity;
										map[entity.name].extensions[extendsToKey].create(extendsTo[extendsToKey], function(err, createdExtension) {
											if (err) throw err;
											console.log('set extension: ', extendsToKey);
										});
									});
								});
							};
							
							
							let hasOne = defaultDatum.hasOne;
							let processHasOnes = async function(getHasOneData) {
								let hasOneKeys = Object.keys(hasOne);
								if (exists(hasOneKeys)) {
									for (let i = 0; i < hasOneKeys.length; i++) {
										
										let hasOneKey = hasOneKeys[i];
										try {
											let promiseData = await getHasOneData(hasOne[hasOneKey].id, hasOneMap[hasOneKey]);
											values[hasOneKey] = promiseData;
											values[hasOneKey + '_id'] = promiseData.id;
										}
										catch (error) {
											console.log('Error adding hasOne default data: ', error);
										}
									}
								}
								createEntity(values);
							};
							
							if (exists(hasOne)) {
								processHasOnes((id, model) => {
									return new Promise(function(resolve, reject) {
										model.find( { id }, function(err, rows) {
											if(err) {
												reject(err);
											}
											resolve(rows[0]);
										});
									});
								});
							} else {
								createEntity(values);
							}

						}
					});
				};

				iterate(entities, function(entity) {
					iterate(entity.defaultData, (defaultDatum) => {
						processDatum(entity, defaultDatum);
					});

					iterate(entity.uniqueConstraints, (uniqueConstraint) => {
						if (isListy(uniqueConstraint.columns)) {

							console.info('Creating unique constraint: ' + entity.name);
							yourSql.createUniqueConstraint(database, entity.name, uniqueConstraint.columns, (err) => {
								if (err && err.indexOf['already exists'] < 0)
									console.log('Error creating unique constraint: ' + JSON.stringify(err));
							});
						}
					});

				});

				iterate(entities, function(entity) {
					iterate(entity.hasMany, (other) => {
						if (exists(map[other.name]) && exists(map[other.name].model)) {
							if (other.options.key) {
								const jointTableName = entity.name + "_" + other.desc;

								console.info('Creating unique constraint during hasMany processing: ' + jointTableName);
								yourSql.createUniqueConstraint(database, jointTableName, [other.name + "_id", entity.name + "_id"], (err) => {
									if (err && err.indexOf['already exists'] < 0)
										console.error('Error creating unique constraint during hasMany processing: ' + JSON.stringify(err));
								});
							}
						}
					});
				});
				
				console.log('finishing sync after attempted db sync');
				callback(err);
			});
		});
	}
}


module.exports = function(conf) {
	return new Storming(conf);
};
