use('company_one');

// 按公司分组，查询所有 job_url
const results = db.getCollection('feishu').aggregate([
  { $match: { job_url: { $exists: true, $ne: "" } } },
  { $group: {
      _id: "$company_name",
      job_urls: { $push: "$job_url" },
      count: { $sum: 1 }
  }},
  { $sort: { count: -1 } }
]).toArray();

let totalJobs = 0;
results.forEach(item => {
  totalJobs += item.count;
  print(`\n${'='.repeat(60)}`);
  print(`公司: ${item._id}  (共 ${item.count} 个职位)`);
  print(`${'='.repeat(60)}`);
  item.job_urls.forEach((url, i) => {
    print(`  ${i + 1}. ${url}`);
  });
});

print(`\n${'='.repeat(60)}`);
print(`汇总: 共 ${results.length} 家公司, ${totalJobs} 个职位`);
print(`${'='.repeat(60)}`);
