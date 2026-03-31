/* global use, db */
// MongoDB Playground
// 查找在 exsiting_company 中存在、但 _id 不在 jobs 中出现的公司

use('crawler');

// 用 $lookup 左反连接：exsiting_company._id 对应 jobs._id
const missing = db.getCollection('exsiting_company').aggregate([
  {
    $lookup: {
      from: 'jobs',
      localField: '_id',
      foreignField: '_id',
      as: 'matched_jobs'
    }
  },
  {
    $match: { matched_jobs: { $eq: [] } }
  },
  {
    $project: { matched_jobs: 0 }
  },
  {
    $sort: { company_name: 1 }
  }
]).toArray();

print(`\nexsiting_company 中有 ${missing.length} 家公司的 _id 未出现在 jobs 中\n`);
print('='.repeat(60));

missing.forEach((doc, i) => {
  print(`  ${i + 1}. [${doc._id}] ${doc.company_name}`);
});

print('='.repeat(60));
print(`共 ${missing.length} 条`);
