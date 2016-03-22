using System;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Lixar.Azure.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Lixar.Azure.Tests
{
    [TestClass]
    public class CloudLockTests
    {
        // Make sure the Storage Emulator is running when using Development Storage Connection String
        private static string storageConnectionString = "UseDevelopmentStorage=true";
        private static string containerName = "testcontainer";
        private static string blobName = "testblob";
        private CloudBlockBlob blob;
        private CloudBlobContainer container;

        [TestInitialize]
        public void Initialize()
        {
            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            container = blobClient.GetContainerReference(containerName);
            container.CreateIfNotExists();

            blob = container.GetBlockBlobReference(blobName);
            blob.UploadText("0");
        }

        [TestCleanup]
        public void Cleanup()
        {
            blob?.Delete();
            container?.Delete();
        }
        
        [TestMethod]
        public async Task CloudLock_ShouldRelease_WhenDisposed()
        {
            using (var cloudLock = new CloudLock(blob, CloudLock.MinimumAcquireTimeSpan))
            {
                Assert.IsTrue(await cloudLock.AcquireAsync(5, TimeSpan.FromSeconds(1)));
            }

            //Lock should be acquired
            var acquiredLeaseId = await blob.AcquireLeaseAsync(CloudLock.MinimumAcquireTimeSpan, null);
            Assert.IsNotNull(acquiredLeaseId);
            await blob.ReleaseLeaseAsync(AccessCondition.GenerateLeaseCondition(acquiredLeaseId));
        }

        [TestMethod]
        public async Task CloudLock_ShouldRenewLock_WhenPastExpiration()
        {
            var acquireTimeSpan = CloudLock.MinimumAcquireTimeSpan;
            using (var cloudLock = new CloudLock(blob, acquireTimeSpan))
            {
                Assert.IsTrue(await cloudLock.TryAcquireAsync());
                
                //Wait one second longer than lock
                await Task.Delay(acquireTimeSpan.Add(TimeSpan.FromSeconds(1)));
                try
                {
                    //Expecting 409 error 
                    await blob.AcquireLeaseAsync(CloudLock.MinimumAcquireTimeSpan, null);
                }
                catch (StorageException ex)
                {
                    Assert.AreEqual(ex.RequestInformation.HttpStatusCode, (int)HttpStatusCode.Conflict);
                }
            }
        }

        [TestMethod]
        public async Task CloudLock_ShouldRetryAcquireLease_WhenCollisions()
        {
            await Task.WhenAll(
                IncrementByOneAsync(),
                IncrementByOneAsync(),
                IncrementByOneAsync());

            var finalText = await blob.DownloadTextAsync();
            Assert.AreEqual(int.Parse(finalText), 3);
        }

        private async Task IncrementByOneAsync()
        {
            using (var cloudLock = new CloudLock(blob))
            {
                Assert.IsTrue(await cloudLock.AcquireAsync(10, TimeSpan.FromSeconds(1)));
                var text = await blob.DownloadTextAsync(Encoding.Default, AccessCondition.GenerateLeaseCondition(cloudLock.LeaseId), null, null);
                var currentCount = int.Parse(text) + 1;
                await blob.UploadTextAsync(currentCount.ToString(), Encoding.Default, AccessCondition.GenerateLeaseCondition(cloudLock.LeaseId), null, null);
                await cloudLock.ReleaseLeaseAsync();
            }
        }
    }
}