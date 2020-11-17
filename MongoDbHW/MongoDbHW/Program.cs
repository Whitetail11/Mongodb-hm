using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Linq;

namespace MongoDbHW
{
    public class Repository<TEntity> where TEntity : IEntity
    {
        protected readonly IMongoCollection<TEntity> collection;
        public Repository(IMongoDatabase database)
        {
            collection = database.GetCollection<TEntity>(typeof(TEntity).Name);
        }
        public ObjectId Insert(TEntity entity)
        {
            collection.InsertOne(entity);
            return entity.Id;
        }

        public TEntity Get(ObjectId id)
        {
            var filter = Builders<TEntity>.Filter.Eq(e => e.Id, id);
            return collection.Find(filter).FirstOrDefault();
        }
    }
    public class PetRepository : Repository<Pet>
    {
        public PetRepository(IMongoDatabase database) : base(database)
        {
            collection.Indexes.CreateOne(new CreateIndexModel<Pet>("{DateOfRegistration: 1}"));
            collection.Indexes.CreateOne(new CreateIndexModel<Pet>("{Type: 1}"));
            if (collection.EstimatedDocumentCount() == 0)
            {
                SetValues();
            }
        }
        public void ShowCollectionByDate(int pages)
        {
            var filter = new BsonDocument();
            var res = collection.Find(filter).Sort("{DateOfRegistration: -1}").Limit(pages*3).ToList();
            foreach(var value in res)
            {
                Console.WriteLine($"{value.Name} {value.Type} {value.DateOfRegistration} ");
                Console.WriteLine($"Owner: {value.OwnerInformation.Name} {value.OwnerInformation.Number}");
                Console.WriteLine();
            }
        }   
        public void getAggregate()
        {
            var res = collection.Aggregate()
                .Group(x => x.Type, g => new
                {
                    Type = g.Key,
                    Count = g.Count()
                }).ToList();
            foreach(var value in res)
            {
                Console.WriteLine(value.ToJson());
            }
        }
        private void SetValues()
        {
            var pet1 = new Pet()
            {
                Name = "pet1",
                Type = "dog",
                DateOfRegistration = new DateTime(2012,10,11),
                OwnerInformation = new OwnerInformation()
                {
                    Name = "Danil",
                    Number = "0501073094"
                }
            };
            var pet2 = new Pet()
            {
                Name = "pet2",
                Type = "cat",
                DateOfRegistration = new DateTime(2013, 10, 11),
                OwnerInformation = new OwnerInformation()
                {
                    Name = "Danil",
                    Number = "0501073094"
                }
            };
            var pet3 = new Pet()
            {
                Name = "pet3",
                Type = "bird",
                DateOfRegistration = new DateTime(2014, 10, 11),
                OwnerInformation = new OwnerInformation()
                {
                    Name = "Danil",
                    Number = "0501073094"
                }
            };
            var pet4 = new Pet()
            {
                Name = "pet4",
                Type = "dog",
                DateOfRegistration = new DateTime(2015, 10, 11),
                OwnerInformation = new OwnerInformation()
                {
                    Name = "Danil",
                    Number = "0501073094"
                }
            };
            var pet5 = new Pet()
            {
                Name = "pet5",
                Type = "cat",
                DateOfRegistration = new DateTime(2016, 10, 11),
                OwnerInformation = new OwnerInformation()
                {
                    Name = "Danil",
                    Number = "0501073094"
                }
            };
            var pet6 = new Pet()
            {
                Name = "pet6",
                Type = "bird",
                DateOfRegistration = new DateTime(2017, 10, 11),
                OwnerInformation = new OwnerInformation()
                {
                    Name = "Danil",
                    Number = "0501073094"
                }
            };
            var pet7 = new Pet()
            {
                Name = "pet7",
                Type = "dog",
                DateOfRegistration = new DateTime(2018, 10, 11),
                OwnerInformation = new OwnerInformation()
                {
                    Name = "Danil",
                    Number = "0501073094"
                }
            };
            var pet8 = new Pet()
            {
                Name = "pet8",
                Type = "cat",
                DateOfRegistration = new DateTime(2019, 10, 11),
                OwnerInformation = new OwnerInformation()
                {
                    Name = "Danil",
                    Number = "0501073094"
                }
            };
            var pet9 = new Pet()
            {
                Name = "pet9",
                Type = "bird",
                DateOfRegistration = new DateTime(2020, 10, 11),
                OwnerInformation = new OwnerInformation()
                {
                    Name = "Danil",
                    Number = "0501073094"
                }
            };
            var pet10 = new Pet()
            {
                Name = "pet10",
                Type = "dog",
                DateOfRegistration = new DateTime(2011, 10, 11),
                OwnerInformation = new OwnerInformation()
                {
                    Name = "Danil",
                    Number = "0501073094"
                }
            };
            this.Insert(pet1);
            this.Insert(pet2);
            this.Insert(pet3);
            this.Insert(pet4);
            this.Insert(pet5);
            this.Insert(pet6);
            this.Insert(pet7);
            this.Insert(pet8);
            this.Insert(pet9);
            this.Insert(pet10);
        }

    }
    public interface IEntity
    {
        public ObjectId Id { get; set; }
    }
    public class Pet : IEntity
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; }
        public string Type { get; set; }
        public DateTime DateOfRegistration { get; set; }
        public OwnerInformation OwnerInformation { get; set; }
    }
    public class OwnerInformation
    {
        public string Name { get; set; }
        public string Number { get; set; }
    }
    class Program
    {
        static void Main(string[] args)
        {
            var client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("PetDb");
            var repo = new PetRepository(database);
            repo.ShowCollectionByDate(4);
            repo.getAggregate();
        }
    }
}